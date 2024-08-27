import json
import boto3
import os

# Initialize the Lambda client using boto3
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    """
    Lambda function handler for collecting data from different sources based on a specified category and optional custom UUID.
    
    Parameters:
    - event: The incoming event containing query string parameters 'category' and optionally 'custom_uuid'.
    - context: Provides runtime information to the handler.

    The function validates the query parameters, loads data sources from a configuration file, 
    and then invokes destination Lambda functions for data collection.
    """

    # Load data sources from configuration file
    data_sources = load_data_sources()
    
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
            # Return a 400 response if the 'category' parameter is missing
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
        # Return a 400 response if no query string parameters are found
        return {
            'statusCode': 400,
            'body': json.dumps("No query string parameters found."),
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
            }
        }

    # Invoke destination Lambda functions with category and custom_uuid if they exist
    for source in data_sources['sources']:
        invoke_destination_lambda(category, custom_uuid, source['lambda_name'])

    # Return a success response after invoking the Lambda functions
    return {
        'statusCode': 200,
        'body': json.dumps('Success collect data call'),
        'headers': {
            'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',  # Allowed HTTP methods
        }
    }

def load_data_sources():
    """
    Loads the data sources configuration from a JSON file.

    Returns:
    - A dictionary containing information about the data sources.
    """
    with open('data_sources.json', 'r') as file:
        return json.load(file)

def invoke_destination_lambda(category, custom_uuid, lambda_name):
    """
    Invokes the specified destination Lambda function asynchronously with the provided category and optional custom UUID.

    Parameters:
    - category: The category of data being collected.
    - custom_uuid: An optional custom UUID for the data collection.
    - lambda_name: The name of the destination Lambda function to invoke.
    """

    # Prepare the payload for the destination Lambda function
    payload = {
        'category': category
    }
    if custom_uuid:
        payload['custom_uuid'] = custom_uuid
        print(f"Custom collect data call with UUID {custom_uuid}")

    # Invoke the destination Lambda function asynchronously
    lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
