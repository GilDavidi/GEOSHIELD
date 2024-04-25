import json
import boto3

def load_json_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def filter_messages_by_category(messages, category):
    return [message for message in messages if message.get("classification") == category]

def lambda_handler(event, context):
    # S3 bucket names and file keys
    classified_bucket = "classified-data-geoshield"
    matching_bucket = "maching-events-geoshield"
    gdelt_file_key = "gdelt_articles_classified.json"
    telegram_file_key = "telegram_messages_classified.json"
    matching_file_key = "matching_messages.json"

    # Get the category from the query parameters
    if 'queryStringParameters' in event:
        # Access the 'category' parameter from the 'queryStringParameters' object
        category = event['queryStringParameters'].get('category')
        
        # Check if 'category' parameter exists
        if category:
            print("Category:", category)
        else:
            return {
                'statusCode': 400,
                'body': "Category parameter not found in query string.",
                'headers': {
                    'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
                }
            }
    else:
        return {
            'statusCode': 400,
            'body': "No query string parameters found.",
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }

    try:
        # Load JSON files from the first S3 bucket
        gdelt_articles = load_json_from_s3(classified_bucket, gdelt_file_key)
        telegram_messages = load_json_from_s3(classified_bucket, telegram_file_key)

        # Filter messages by category
        gdelt_articles_filtered = filter_messages_by_category(gdelt_articles, category)
        telegram_messages_filtered = filter_messages_by_category(telegram_messages, category)

        # Load JSON file from the second S3 bucket
        matching_messages = load_json_from_s3(matching_bucket, matching_file_key)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'gdelt_articles': gdelt_articles_filtered,
                'telegram_messages': telegram_messages_filtered,
                'matching_messages': matching_messages
            }),
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }
