import json
import boto3
import concurrent.futures
from datetime import datetime
import requests
import traceback

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
    prompt =  message + question
    # Request headers
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Request payload
    request_body = {
        "prompt": prompt,
        "maxTokens": 250,  # Specify the maximum number of tokens in the generated completion
    }
    
    # Send the request to the AI21 API
    response = requests.post(endpoint, data=json.dumps(request_body), headers=headers)
    
    # Parse the response JSON
    response_json = response.json()
    
    # Initialize generated_text
    generated_text = ""
    # Check if 'completions' key exists in the response
    if 'completions' in response_json:
        # Extract the generated text from the response
        generated_text = response_json['completions'][0]['data']['text']
        # Clean up the response text
        generated_text = generated_text.strip()
        
    # Print the cleaned location
    print(f"event_breakdown: {generated_text}")
    
    return generated_text

def process_message(message, category):

    if message["message"]:
        print("Processing message:", message["message"])
        
        event_breakdown = None
        
        event_breakdown_question = f"What are the {category} events in the text? Write it only in English, in the form of a list of reports (without conclusions and without some summary sentence after the events or title) (For me, an event is an event that can be placed on a map - that is, with a location). If several events are related to each other, list them as a single event in brief"
        # Define maximum number of attempts to find a valid location
        max_attempts = 3
        attempt_count = 0
        
        while attempt_count < max_attempts:
            event_breakdown = generate_text(message["message"], event_breakdown_question)
            
            # Check if the location is meaningful, not empty, and does not contain specific words
            if event_breakdown != "":
                message["event_breakdown"] = str(event_breakdown)
                print("event_breakdown found:", event_breakdown)
                break  # Exit loop if a valid location is found
            else:
                attempt_count += 1  # Increment attempt count
                
        # If no valid location is found after maximum attempts, default to 'null'
        if attempt_count == max_attempts:
            message["event_breakdown"] = "null"
            print("No valid event_breakdown found for the message. Message dropped.")
            
    return message
    
def compare_first_two_words(file1, file2):
    # Split the file names by underscore
    words1 = file1.split('_')
    words2 = file2.split('_')
    
    # Compare the first two words
    return words1[0] == words2[0] and words1[1] == words2[1]


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
        s3 = boto3.client('s3')
        if event['bucket_name'] == 'raw-data-geoshield':
            bucket_name = 'classified-data-geoshield'
        else:
            bucket_name = 'custom-classified-data-geoshield'
        category = event['category']
        file_name = event['file_name']
        messages = event['messages']
        today = datetime.now().strftime('%Y-%m-%d')
        today_with_time = datetime.now().strftime('%Y-%m-%d %H:%M')  # Printing today's date with current hour and minute
        print("Today's date with time:", today_with_time)
        file_exists_today = False
        matching_category = None
     
        print("Extracting data from " + file_name + " started")
            
        # Define the maximum number of workers for concurrent processing
        max_workers = 10
            
            # Process messages concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_messages = list(executor.map(process_message, messages, [category] * len(messages)))
        
        if bucket_name == 'classified-data-geoshield':
            # List all files in the S3 bucket
            response = s3.list_objects_v2(Bucket=bucket_name)
            
            # Check if any file matches today's date and category
            for obj in response.get('Contents', []):
                file_key = obj['Key']
                last_modified = obj['LastModified'].strftime('%Y-%m-%d')
                last_modified_with_time = obj['LastModified'].strftime('%Y-%m-%d %H:%M')  # Printing last modified date with current hour and minute
                print("Last modified date with time for file", file_key + ":", last_modified_with_time)
                # Check if the file's last modified date matches today's date
                if today == last_modified:
                    if compare_first_two_words(file_key,file_name):
                        response_tags = s3.get_object_tagging(Bucket=bucket_name, Key=file_key)
                        print("found file: "+ file_key)
                        for tag in response_tags['TagSet']:
                            print(tag)
                            # Check if the file's category matches the event's category
                            if tag['Key'] == 'Category':
                                if tag['Value'] == event['category']:
                                    file_exists_today = True
                                    matching_category = tag['Value']
                                    break
                if file_exists_today:
                    break
            
            # If a matching file exists, append new information to it
            if file_exists_today:
                # Download the file from S3
                response = s3.get_object(Bucket=bucket_name, Key=file_key)
                file_content = response['Body'].read().decode('utf-8')
                existing_messages = json.loads(file_content)
                
                # Filter out existing messages from the processed messages
                all_message = existing_messages+processed_messages
                updated_messages = remove_duplicate_urls(all_message)
    
                # Upload the updated content back to S3
                s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(updated_messages))
                
                # Add category tag to the file in S3
                s3.put_object_tagging(
                    Bucket=bucket_name,
                    Key=file_key,
                    Tagging={
                        'TagSet': [
                            {
                                'Key': 'Category',
                                'Value': category
                            }
                        ]
                    }
                )
                print(f"Data appended to {file_key} successfully!")
                
                
            else:
                # Proceed with regular process of saving processed messages as a new file in S3
                s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(processed_messages))
                # Add category tag to the file in S3
                s3.put_object_tagging(
                    Bucket=bucket_name,
                    Key=file_name,
                    Tagging={
                        'TagSet': [
                            {
                                'Key': 'Category',
                                'Value': category
                            }
                        ]
                    }
                )
                print("New file data  " + file_name + " processed and saved successfully!")
        
        else:
          # Proceed with regular process of saving processed messages as a new file in S3
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(processed_messages))
            # Add category tag to the file in S3
            s3.put_object_tagging(
                Bucket=bucket_name,
                Key=file_name,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'Category',
                            'Value': category
                        }
                    ]
                }
            )
            print("New file data  " + file_name + " processed and saved successfully!")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Files processed and saved successfully!')
        }
    except Exception as e:
        print("Error: " + str(e))
        traceback.print_exc()  
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }