import json
import boto3
from datetime import datetime

def load_json_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def filter_messages_by_category(messages, category):
    return [message for message in messages if message.get("classification") == category]

def remove_duplicate_urls(messages):
    unique_urls = set()
    unique_messages = []
    for message in messages:
        url = message.get('url')
        if url not in unique_urls:
            unique_urls.add(url)
            unique_messages.append(message)
    return unique_messages

def lambda_handler(event, context):
    try:
        # S3 bucket name
        classified_bucket = "classified-data-geoshield"

        # Get the query parameters
        query_params = event.get('queryStringParameters', {})
        category = query_params.get('category')
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')

        # Check if 'category' parameter exists
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

        # Get the list of objects in the S3 bucket
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=classified_bucket)

        # Filter files by last modified date and category
        filtered_files = []
        if 'Contents' in response:
            for file in response['Contents']:
                last_modified = file['LastModified']
                last_modified_date = last_modified.strftime('%Y-%m-%d')
                if start_date <= last_modified_date <= end_date and category in file['Key']:
                    filtered_files.append(file['Key'])

        # Load JSON files from S3 bucket
        filtered_messages = []
        for file_key in filtered_files:
            json_data = load_json_from_s3(classified_bucket, file_key)
            # Filter out messages occurred before start_date
            json_data_filtered = [msg for msg in json_data if msg.get('date') >= start_date]
            filtered_messages.extend(json_data_filtered)

        # Remove duplicate messages based on URL
        unique_messages = remove_duplicate_urls(filtered_messages)

        return {
            'statusCode': 200,
            'body': json.dumps(unique_messages),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }
