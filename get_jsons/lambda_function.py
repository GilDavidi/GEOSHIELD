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
        # Print received event and query parameters
        print("Received event:", json.dumps(event))
        
        # S3 bucket names
        classified_bucket = "classified-data-geoshield"
        matching_bucket = "maching-events-geoshield"

        # Get the query parameters
        query_params = event.get('queryStringParameters', {})
        print("Query Parameters:", query_params)
        category = query_params.get('category')
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')

        # Check if 'category' parameter exists
        if not category:
            # Return a 400 status code if category parameter is missing
            return {
                'statusCode': 400,
                'body': "Category parameter not found in query string.",
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
                }
            }

        # Get the list of objects in the S3 buckets including their tags
        s3 = boto3.client('s3')
        response_classified = s3.list_objects_v2(Bucket=classified_bucket, FetchOwner=True)

        # Filter files by last modified date and category for classified bucket
        filtered_files = []
        if 'Contents' in response_classified:
            for file in response_classified['Contents']:
                last_modified = file['LastModified']
                last_modified_date = last_modified.strftime('%Y-%m-%d')
                if start_date <= last_modified_date <= end_date:
                    # Get tags for the object
                    tags_response = s3.get_object_tagging(Bucket=classified_bucket, Key=file['Key'])
                    object_tags = tags_response.get('TagSet', [])
                    # Check if the object has a 'category' tag and it matches the specified category
                    if any(tag['Key'] == 'Category' and tag['Value'] == category for tag in object_tags):
                        filtered_files.append(file['Key'])

        # Load JSON files from classified bucket
        gdelt_articles = []
        telegram_messages = []
        for file_key in filtered_files:
            # Print loading file from S3
            print("Loading file from S3:", file_key)
            json_data = load_json_from_s3(classified_bucket, file_key)
            # Filter out messages occurred before start_date
            json_data_filtered = [msg for msg in json_data if msg.get('date') >= start_date]
            if 'gdelt' in file_key:
                gdelt_articles.extend(json_data_filtered)
            elif 'telegram' in file_key:
                telegram_messages.extend(json_data_filtered)

        # Remove duplicate messages based on URL
        gdelt_articles = remove_duplicate_urls(gdelt_articles)
        telegram_messages = remove_duplicate_urls(telegram_messages)

        # Load JSON file from matching bucket
        matching_messages = []
        matching_file_key = "matching_messages.json"
        print("Loading file from S3:", matching_file_key)
        matching_json_data = load_json_from_s3(matching_bucket, matching_file_key)

        # Prepare response body
        response_body = {
            'gdelt_articles': gdelt_articles,
            'telegram_messages': telegram_messages,
            'matching_messages':  matching_json_data
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
        # Print and return error message with 500 status code
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
        "category": "example_category",
        "start_date": "2024-01-01",
        "end_date": "2024-05-01"
    }
}
lambda_handler(event, None)