import requests
import json
import concurrent.futures
import boto3
import traceback
from botocore.exceptions import ClientError

# Define the correct endpoint for AI21 Studio's API
endpoint = "https://api.ai21.com/studio/v1/j2-ultra/complete"

# AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager')

secret_name = "ai_api_secrets"
region_name = "eu-west-1"

get_secret_value_response = secrets_client.get_secret_value(
    SecretId=secret_name
)
secret = json.loads(get_secret_value_response['SecretString'])
api_key = secret['api_key']

def generate_text(message, question):
    prompt = message + question
    # Request headers
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Request payload
    request_body = {
        "prompt": prompt,
        "maxTokens": 16,  # Specify the maximum number of tokens in the generated completion
    }
    
    # Send the request to the AI21 API
    response = requests.post(endpoint, data=json.dumps(request_body), headers=headers)
    
    # Parse the response JSON
    response_json = response.json()
    
    # Initialize generated_text
    generated_text = ""
    
    # Check if 'completions' key exists in the response
    if 'completions' in response_json:
        
        print("response_json: ", response_json)
        # Extract the generated text from the response
        generated_text = response_json['completions'][0]['data']['text']
        
        # Clean up the response text
        generated_text = generated_text.strip()
    
    print("generated_text: ", generated_text)
    # Check if the response contains 'Location:null'
    if 'Location:null' in generated_text:
        location = 'null'
    else:
        # Extract location after 'Location:'
        location = generated_text.split('Location:')[-1].strip() if generated_text else 'null'

    # Print the cleaned location
    print(f"Location: {location}")

    return location

def process_message(message):
    
    
    if message["message"]:
        print("Processing message:", message["message"])
        
        location = None
        
        location_question = "Based on the information in the text, please identify the main location where the main event takes place. Choose only one location that accurately represents the main focus of the event. Be sure to find the most specific location (eg city and state and not just a country and so on). (Answer in English Only the location you found. Answer for example: 'ODESA', note that there may not be a central event that can be placed on a map - if you cannot determine a specific location, answer only the exact word- null without any additions to the word"     
        # Define maximum number of attempts to find a valid location
        max_attempts = 3
        attempt_count = 0
        
        while attempt_count < max_attempts:
            location = generate_text(message["message"], location_question)
            
            # Check if the location is meaningful, not empty, and does not contain specific words
            if location and 'null' not in location and location != "":
                message["location"] = str(location)
                print("Location found:", location)
                break  # Exit loop if a valid location is found
            else:
                attempt_count += 1  # Increment attempt count
                
        # If no valid location is found after maximum attempts, default to 'null'
        if attempt_count == max_attempts:
            message["location"] = "null"
            print("No valid location found for the message. Message dropped.")
            
    return message


def lambda_handler(event, context):
    try:

        # Extracting S3 bucket name and object key from the SQS message
        message_body = json.loads(event['Records'][0]['body'])
        inner_message_body = json.loads(message_body['Message'])
        bucket_name = inner_message_body['Records'][0]['s3']['bucket']['name']
        print("bucket - " + bucket_name)
        file_name = inner_message_body['Records'][0]['s3']['object']['key']
        
        print("Extracting location for " + file_name + " started")
        
        # Initialize Boto3 client for S3
        s3 = boto3.client('s3')
        
        # Read JSON data from input S3 bucket
        if bucket_name == 'raw-data-geoshield':
            response = s3.get_object(Bucket='raw-data-geoshield', Key=file_name)
            tags = s3.get_object_tagging(Bucket='raw-data-geoshield', Key=file_name)
        else:
            response = s3.get_object(Bucket='custom-raw-data-geoshield', Key=file_name)
            tags = s3.get_object_tagging(Bucket='custom-raw-data-geoshield', Key=file_name)            
        
        # Accessing the 'Category' value from the 'TagSet'
        for tag in tags['TagSet']:
            if tag['Key'] == 'Category':
                category = tag['Value']
                break

        print("Category:", category) 
        
        json_data = response['Body'].read().decode('utf-8')
        messages = json.loads(json_data)
        
        max_workers = 10
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_messages = list(filter(lambda x: x is not None, executor.map(process_message, messages)))
            
        # Filter out messages with null location
        processed_messages = [msg for msg in processed_messages if msg["location"] != "null"]
        
        # Invoke another Lambda function to process the results
        lambda_client = boto3.client('lambda')
        print("output messages: " + str(processed_messages))
        invoke_response = lambda_client.invoke(
            FunctionName='data_classification',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "file_name": file_name,
                "messages": processed_messages,
                "category": category,  # Pass the category tag to the next Lambda function
                "bucket_name": bucket_name
            })
        )
        
        print("Extracting location for " + file_name + " completed")
        
        return {
            "statusCode": 200,
            "body": json.dumps({"messages": processed_messages})
        }

    except Exception as e:
        print("Error: " + str(e))
        traceback.print_exc()  
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
