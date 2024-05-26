import json
import boto3

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # Check if the 'queryStringParameters' key exists in the event
    if 'queryStringParameters' in event:
        # Access the 'category' and 'custom_uuid' parameters from the 'queryStringParameters' object
        query_params = event['queryStringParameters']
        category = query_params.get('category')
        custom_uuid = query_params.get('custom_uuid')

        # Check if 'category' parameter exists
        if category:
            print("New Data Collection request with category:", category)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps("Category parameter not found in query string."),
                'headers': {
                    'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
                }
            }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps("No query string parameters found."),
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }

    # Invoke destination Lambda functions with category and custom_uuid if exists
    invoke_destination_lambda(category, custom_uuid, 'GDELT_data_collection')
    invoke_destination_lambda(category, custom_uuid, 'telegram_data_collection')

    return {
        'statusCode': 200,
        'body': json.dumps('Success collect data call'),
        'headers': {
            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
        }
    }

def invoke_destination_lambda(category, custom_uuid, lambda_name):
    # Prepare payload for the destination Lambda function
    payload = {
        'category': category
    }
    if custom_uuid:
        payload['custom_uuid'] = custom_uuid
        print(f"custom collect data call with uuid{custom_uuid}")

    # Invoke the destination Lambda function
    lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
