import json
import boto3
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # Check if the 'queryStringParameters' key exists in the event
    if 'queryStringParameters' in event:
        # Access the 'category' parameter from the 'queryStringParameters' object
        category =  event['queryStringParameters'].get('category')
        
        # Check if 'category' parameter exists
        if category:
            print("New Data Collection request with category:", category)
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

    invoke_destination_lambda(category, 'GDELT_data_collection')
    invoke_destination_lambda(category, 'telegram_data_collection')

    return {
        'statusCode': 200,
        'body': json.dumps('Success collect data call'),
        'headers': {
            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
        }
    }

def invoke_destination_lambda(category, lambdaName):
    # Invoke the destination Lambda function
    lambda_client.invoke(
        FunctionName=lambdaName,
        InvocationType='Event', 
        Payload=json.dumps({'category': category})
    )
