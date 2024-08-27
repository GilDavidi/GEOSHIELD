import json
import uuid
import boto3
from datetime import datetime, timedelta, timezone
import traceback
import re

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    """
    AWS Lambda function handler to process configuration requests.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The context object passed to the Lambda function.

    Returns:
        dict: The response containing the status code, body, and headers.
    """
    try:
        # Print the received event for debugging purposes
        print("Received event:", json.dumps(event))
        
        # Parse incoming JSON data from the API request
        request_body = json.loads(event['body'])
        
        # Validate JSON structure
        if 'Telegram_Channels' not in request_body or 'GDELT_Domains' not in request_body or 'category' not in request_body:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON structure'}),
                'headers': {
                    'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
                }
            }
        
        # Extract the category from the request body
        category = request_body['category']
        
        # Convert JSON data to string for storage
        config_data = json.dumps(request_body)
        
        # Define the S3 bucket name
        bucket_name = 's3-files-geoshield'
        
        # Calculate the time range (last two hours)
        current_time = datetime.now(timezone.utc)
        past_time = current_time - timedelta(hours=2)
        
        # List objects in the S3 bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        
        # Iterate over objects to check for matching configurations
        for obj in objects.get('Contents', []):
            obj_key = obj['Key']
            
            # Check if the object key follows the desired pattern
            if not re.match(r'^channels_config_[a-f\d]{8}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{12}\.json$', obj_key):
                # Skip processing if the key doesn't match the pattern
                continue
            
            # Check if the object is within the last two hours
            obj_metadata = s3_client.head_object(Bucket=bucket_name, Key=obj_key)
            obj_last_modified = obj_metadata['LastModified']
            if obj_last_modified >= past_time:
                # Get object content
                obj_body = s3_client.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read()
                obj_content = obj_body.decode('utf-8', errors='replace')  # Handle decoding errors
                obj_data = json.loads(obj_content)
                
                # Compare the content with the request body
                if (obj_data['category'] == request_body['category'] and
                    obj_data['Telegram_Channels'] == request_body['Telegram_Channels'] and
                    obj_data['GDELT_Domains'] == request_body['GDELT_Domains']):
                    # Extract UUID from the file name
                    unique_id = obj_key.split('_')[-1].split('.')[0]
                    
                    # Return existing configuration ID
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': 'Configuration already exists',
                            'uuid': unique_id,
                            'category': category,
                            'exists': True
                        }),
                        'headers': {
                            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
                        }
                    }
        
        # No existing configuration found, proceed to save the new configuration
        unique_id = str(uuid.uuid4())
        object_key = f'channels_config_{unique_id}.json'
        
        # Upload JSON data to S3
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=config_data)
        
        # Prepare the payload for invoking another Lambda function
        payload = {
            'queryStringParameters': {
                'category': category,
                'custom_uuid': unique_id
            }
        }
        
        # Invoke the 'data_collection' Lambda function
        response = lambda_client.invoke(
            FunctionName='data_collection',
            InvocationType='RequestResponse',  # Synchronous invocation
            Payload=json.dumps(payload)
        )
        
        # Parse the response from the 'data_collection' Lambda function
        response_payload = json.loads(response['Payload'].read())
        
        # Return success response with UUID and category
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Configuration saved successfully and data_collection Lambda called',
                'uuid': unique_id,
                'category': category,
                'exists': False,
                'data_collection_response': response_payload
            }),
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }
    except Exception as e:
        # Log the error and traceback
        print("Error: " + str(e))
        traceback.print_exc()  
        # Return error response
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }
