import json
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
BUCKET_NAME = 's3-files-geoshield'

def lambda_handler(event, context):
    path = event.get('rawPath')
    
    if path == '/get-categories':
        file_key = 'categories.json'
    elif path == '/get-sources':
        file_key = 'config.json'
    else:
        return {
            'statusCode': 404,
            'body': json.dumps('Not Found'),
            'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }
    
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        
        return {
            'statusCode': 200,
            'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            },
            'body': file_content
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {e}'),
            'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }
