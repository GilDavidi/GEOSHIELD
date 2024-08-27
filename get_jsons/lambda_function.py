import json
import boto3
import re
from datetime import datetime

def load_json_from_s3(bucket_name, file_key):
    """
    Load a JSON file from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) of the file in the S3 bucket.

    Returns:
        dict: The JSON content of the file.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def extract_uuid(filename):
    """
    Extract a UUID from a filename.

    Args:
        filename (str): The filename from which to extract the UUID.

    Returns:
        str or None: The extracted UUID if found, otherwise None.
    """
    pattern = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
    match = re.search(pattern, filename)
    if match:
        return match.group(0)
    else:
        return None

def lambda_handler(event, context):
    """
    AWS Lambda function handler to process requests for classified and matching files.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The context object passed to the Lambda function.

    Returns:
        dict: The response containing the status code, body, and headers.
    """
    try:
        # Print the received event for debugging purposes
        print("Received event:", json.dumps(event, indent=2))

        # Extract query parameters from the event
        query_params = event.get('queryStringParameters', {})
        print("Query Parameters:", query_params)

        # Extract the UUID parameter from the query parameters
        uuid_param = query_params.get('uuid')
        print("UUID Parameter:", uuid_param)

        classified_bucket = "classified-data-geoshield"
        matching_bucket = "maching-events-geoshield"

        # If UUID is provided, use custom buckets
        if uuid_param:
            classified_bucket = "custom-classified-data-geoshield"
            matching_bucket = "custom-matching-events-geoshield"

        s3 = boto3.client('s3')

        classified_files = []
        matching_files = []

        if uuid_param:
            # Fetch files with the specified UUID from S3
            print("Fetching files with UUID:", uuid_param)
            response_classified = s3.list_objects_v2(Bucket=classified_bucket, FetchOwner=True)
            if 'Contents' in response_classified:
                for file in response_classified['Contents']:
                    file_uuid = extract_uuid(file['Key'])
                    if file_uuid == uuid_param:
                        classified_files.append(file['Key'])

            response_matching = s3.list_objects_v2(Bucket=matching_bucket, FetchOwner=True)
            if 'Contents' in response_matching:
                for file in response_matching['Contents']:
                    file_uuid = extract_uuid(file['Key'])
                    if file_uuid == uuid_param:
                        matching_files.append(file['Key'])
        else:
            # Fetch files based on category and date range
            category = query_params.get('category')
            start_date = query_params.get('start_date')
            end_date = query_params.get('end_date')

            print("Category:", category)
            print("Start Date:", start_date)
            print("End Date:", end_date)

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
            if 'Contents' in response_classified:
                for file in response_classified['Contents']:
                    last_modified = file['LastModified']
                    last_modified_date = last_modified.strftime('%Y-%m-%d')
                    print(f"Checking file {file['Key']} with last modified date {last_modified_date}")
                    if start_date <= last_modified_date <= end_date:
                        tags_response = s3.get_object_tagging(Bucket=classified_bucket, Key=file['Key'])
                        object_tags = tags_response.get('TagSet', [])
                        if any(tag['Key'] == 'Category' and tag['Value'] == category for tag in object_tags):
                            classified_files.append(file['Key'])

            response_matching = s3.list_objects_v2(Bucket=matching_bucket, FetchOwner=True)
            if 'Contents' in response_matching:
                for file in response_matching['Contents']:
                    last_modified = file['LastModified']
                    last_modified_date = last_modified.strftime('%Y-%m-%d')
                    print(f"Checking file {file['Key']} with last modified date {last_modified_date}")
                    if start_date <= last_modified_date <= end_date:
                        tags_response = s3.get_object_tagging(Bucket=matching_bucket, Key=file['Key'])
                        object_tags = tags_response.get('TagSet', [])
                        if any(tag['Key'] == 'Category' and tag['Value'] == category for tag in object_tags):
                            matching_files.append(file['Key'])

        print("Classified Files:", classified_files)
        print("Matching Files:", matching_files)

        # Load data from the classified and matching files
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

        # Prepare the response body
        response_body = {
            'gdelt_articles': gdelt_articles,
            'telegram_messages': telegram_messages,
            'matching_messages': matching_messages
        }

        print("Response Body:", json.dumps(response_body, indent=2))

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
