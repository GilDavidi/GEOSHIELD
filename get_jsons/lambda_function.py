import json
import boto3
import re
from datetime import datetime

def load_json_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def extract_uuid(filename):
    pattern = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
    match = re.search(pattern, filename)
    if match:
        return match.group(0)
    else:
        return None

def lambda_handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        query_params = event.get('queryStringParameters', {})
        print("Query Parameters:", query_params)

        uuid_param = query_params.get('uuid')
        
        print(uuid_param)
        
        classified_bucket = "classified-data-geoshield"
        matching_bucket = "maching-events-geoshield"

        if uuid_param:
            classified_bucket = "custom-classified-data-geoshield"
            matching_bucket = "custom-matching-events-geoshield"

        s3 = boto3.client('s3')

        if uuid_param:
            response_classified = s3.list_objects_v2(Bucket=classified_bucket, FetchOwner=True)
            classified_files = []
            if 'Contents' in response_classified:
                for file in response_classified['Contents']:
                    file_uuid = extract_uuid(file['Key'])
                    if file_uuid == uuid_param:
                        classified_files.append(file['Key'])

            response_matching = s3.list_objects_v2(Bucket=matching_bucket, FetchOwner=True)
            matching_files = []
            if 'Contents' in response_matching:
                for file in response_matching['Contents']:
                    file_uuid = extract_uuid(file['Key'])
                    if file_uuid == uuid_param:
                        matching_files.append(file['Key'])
        else:
            category = query_params.get('category')
            start_date = query_params.get('start_date')
            end_date = query_params.get('end_date')

            if not category:
                return {
                    'statusCode': 400,
                    'body': "Category parameter not found in query string.",
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
                    }
                }

            response_classified = s3.list_objects_v2(Bucket=classified_bucket, FetchOwner=True)
            classified_files = []
            if 'Contents' in response_classified:
                for file in response_classified['Contents']:
                    last_modified = file['LastModified']
                    last_modified_date = last_modified.strftime('%Y-%m-%d')
                    if start_date <= last_modified_date <= end_date:
                        tags_response = s3.get_object_tagging(Bucket=classified_bucket, Key=file['Key'])
                        object_tags = tags_response.get('TagSet', [])
                        if any(tag['Key'] == 'Category' and tag['Value'] == category for tag in object_tags):
                            classified_files.append(file['Key'])

            response_matching = s3.list_objects_v2(Bucket=matching_bucket, FetchOwner=True)
            matching_files = []
            if 'Contents' in response_matching:
                for file in response_matching['Contents']:
                    last_modified = file['LastModified']
                    last_modified_date = last_modified.strftime('%Y-%m-%d')
                    if start_date <= last_modified_date <= end_date:
                        tags_response = s3.get_object_tagging(Bucket=matching_bucket, Key=file['Key'])
                        object_tags = tags_response.get('TagSet', [])
                        if any(tag['Key'] == 'Category' and tag['Value'] == category for tag in object_tags):
                            matching_files.append(file['Key'])

        gdelt_articles = []
        telegram_messages = []
        for file_key in classified_files:
            print("Loading file from S3:", file_key)
            json_data = load_json_from_s3(classified_bucket, file_key)
            if 'gdelt' in file_key:
                gdelt_articles.extend(json_data)
            elif 'telegram' in file_key:
                telegram_messages.extend(json_data)

        matching_messages = []
        for file_key in matching_files:
            print("Loading file from S3:", file_key)
            matching_json_data = load_json_from_s3(matching_bucket, file_key)
            matching_messages.append(matching_json_data)

        response_body = {
            'gdelt_articles': gdelt_articles,
            'telegram_messages': telegram_messages,
            'matching_messages': matching_messages
        }

        return {
            'statusCode': 200,
            'body': json.dumps(response_body),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }

# Test locally
event = {
    "queryStringParameters": {
        "uuid": "example-uuid",
        "category": "example_category",
        "start_date": "2024-01-01",
        "end_date": "2024-05-01"
    }
}
lambda_handler(event, None)
