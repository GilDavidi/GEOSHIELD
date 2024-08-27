import json
import boto3
import concurrent.futures
from datetime import datetime
import requests
import traceback

# AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager')

# Secrets Manager configuration
endpoint_secret_name = "ai_api_endpoint"
api_secret_name = "ai_api_secrets"
region_name = "eu-west-1"

# Fetching the endpoint from AWS Secrets Manager
get_secret_value_response = secrets_client.get_secret_value(
    SecretId=endpoint_secret_name
)
secret = json.loads(get_secret_value_response['SecretString'])
endpoint = secret['ai_endpoint']

# Fetching the API key from AWS Secrets Manager
get_secret_value_response = secrets_client.get_secret_value(
    SecretId=api_secret_name
)
secret = json.loads(get_secret_value_response['SecretString'])
api_key = secret['api_key']

def generate_text(message, question):
    """
    Generates a response from the AI21 API based on the provided message and question.

    Args:
        message (str): The message to be analyzed.
        question (str): The question to be asked to the AI21 API.

    Returns:
        str: The generated text from the AI21 API response.
    """
    prompt = message + question
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
    """
    Processes a single message to extract current events relevant to the specified category.

    Args:
        message (dict): The message containing the event details.
        category (str): The category of the event to be identified.

    Returns:
        dict: The message with the added 'event_breakdown' key.
    """
    if message["message"]:
        print("Processing message:", message["message"])
        
        event_breakdown = None
        
        event_breakdown_question = f"Identify and list only current events of {category} that are reported in text that can be placed on a map (ie, have a specific location). Each event should be formatted as a short report in English. Note the target event - a news push that reports an event that is happening in the current time frame and belongs to to the category {category}, and not a past event. If no relevant events were found, return 'null'."
        # Define maximum number of attempts to find a valid event breakdown
        max_attempts = 3
        attempt_count = 0
        
        while attempt_count < max_attempts:
            event_breakdown = generate_text(message["message"], event_breakdown_question)
            
            # Check if the event_breakdown is meaningful, not empty
            if event_breakdown != "":
                message["event_breakdown"] = str(event_breakdown)
                print("event_breakdown found:", event_breakdown)
                break  # Exit loop if a valid event breakdown is found
            else:
                attempt_count += 1  # Increment attempt count
                
        # If no valid event_breakdown is found after maximum attempts, default to 'null'
        if attempt_count == max_attempts:
            message["event_breakdown"] = "null"
            print("No valid event_breakdown found for the message. Message dropped.")
            
    return message

def compare_first_two_words(file1, file2):
    """
    Compares the first two words of two file names to check for a match.

    Args:
        file1 (str): The first file name.
        file2 (str): The second file name.

    Returns:
        bool: True if the first two words of both file names match, otherwise False.
    """
    # Split the file names by underscore
    words1 = file1.split('_')
    words2 = file2.split('_')
    
    # Compare the first two words
    return words1[0] == words2[0] and words1[1] == words2[1]

def remove_duplicate_urls(messages):
    """
    Removes duplicate messages based on URL from a list of messages.

    Args:
        messages (list): A list of message dictionaries, each containing a 'url' key.

    Returns:
        list: A list of messages with duplicates removed.
    """
    unique_urls = set()
    unique_messages = []
    for message in messages:
        url = message.get('url')
        if url not in unique_urls:
            unique_urls.add(url)
            unique_messages.append(message)
    return unique_messages

def lambda_handler(event, context):
    """
    AWS Lambda function handler that processes S3 events to extract and process messages.

    Args:
        event (dict): The event data from the triggering source.
        context (object): The context object providing information about the invocation, function, and execution environment.

    Returns:
        dict: A response indicating the status of the operation.
    """
    try:
        # Initialize Boto3 client for S3
        s3 = boto3.client('s3')
        
        # Determine bucket name based on event source
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

        # Filter out messages with 'null' (in any case) in event_breakdown
        filtered_messages = [msg for msg in processed_messages if "null" not in msg["event_breakdown"].lower()]

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
                    if compare_first_two_words(file_key, file_name):
                        response_tags = s3.get_object_tagging(Bucket=bucket_name, Key=file_key)
                        print("found file: " + file_key)
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
                all_message = existing_messages + filtered_messages
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
                s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(filtered_messages))
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
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(filtered_messages))
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
