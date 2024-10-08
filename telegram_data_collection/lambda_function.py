import os
import json
import asyncio
import configparser
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import PeerChannel, InputMessagesFilterEmpty
from telethon.sessions import StringSession
import boto3
import uuid

# AWS S3 client
s3 = boto3.client('s3')

# AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager')

# Lambda handler function
def lambda_handler(event, context):
    """
    AWS Lambda function handler to fetch Telegram messages and save them to S3.

    Parameters:
    event (dict): Input event data, which may include 'custom_uuid' and 'category'.
    context (object): Lambda context object.

    Returns:
    dict: Response with HTTP status code and message.
    """
    try:
        # Retrieve Telegram secrets from AWS Secrets Manager
        secret_name = "telegram_secrets"
        region_name = "eu-west-1"

        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
        
        secret = json.loads(get_secret_value_response['SecretString'])
        api_id = secret['api_id']
        api_hash = secret['api_hash']
        string_session = secret['string_session']
        
        # Reading Configurations
        config = configparser.ConfigParser()
        config.read("config.ini")
        bucket_name = config['S3']['bucket_name']
        
        # Check if custom selection was performed
        custom_uuid = event.get('custom_uuid', None)
        if custom_uuid:
            # Access the correct file in the S3 bucket
            file_name = f'channels_config_{custom_uuid}.json'
            obj = s3.get_object(Bucket='s3-files-geoshield', Key=file_name)
            file_content = obj['Body'].read().decode('utf-8')
            config_data = json.loads(file_content)
            channels = list(config_data.get("Telegram_Channels", {}).values())
            print(f"Custom UUID found: {custom_uuid}")
            print(f"Channels from config: {channels}")
        else:
            # Default behavior
            channels = ['https://t.me/englishabuali']
            
        category = event['category']

        # Create the TelegramClient using the StringSession
        client = TelegramClient(StringSession(string_session), api_id, api_hash)

        # Running the asyncio loop
        all_messages = asyncio.get_event_loop().run_until_complete(fetch_telegram_messages(client, channels))

        print("Telegram messages fetched.")

        # Create a list to hold selected messages
        selected_messages = []
        
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Iterate through all messages and extract required fields
        for message in all_messages:
            # Exclude empty messages
            if message.get('message'):
                # Generate a unique ID for each message
                unique_id = str(uuid.uuid4())  
                # Extracting and formatting the date to include only date and hour
                
                date_str = message['date'].strftime("%Y-%m-%d %H:%M")
                date_str_format= message['date'].strftime('%Y-%m-%d')
                if date_str_format == today_date:
                    url = extract_url(message.get('entities', []))
                    selected_message = {
                        "id": unique_id,  # Replace the original ID with the unique ID
                        "channel_id": message['peer_id']['channel_id'] if 'peer_id' in message and 'channel_id' in message['peer_id'] else None,
                        "url": url,
                        "date": date_str,
                        "message": message['message']
                    }
                    selected_messages.append(selected_message)

        # Convert the list of selected messages to JSON
        json_data = json.dumps(selected_messages)

        # Determine the folder and file name for saving
        if custom_uuid:
            bucket_name = 'custom-raw-data-geoshield'
            file_name = f'telegram_messages_{custom_uuid}.json'
        else:
            file_name = f'telegram_messages_{str(uuid.uuid4())}.json'

        print(f"Saving file to S3 with name: {file_name}")

        # Upload the JSON data to the S3 bucket with the adjusted file name
        upload_byte_stream = bytes(json_data.encode('UTF-8'))
        response = s3.put_object(Bucket=bucket_name, Key=file_name, Body=upload_byte_stream)

        # Print response from S3
        print(f"S3 put_object response: {response}")

        # Check the response for success
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f"Error uploading file to S3: {response}")

        # Add tags to the uploaded S3 object
        tag_response = s3.put_object_tagging(
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

        # Print response from S3 tagging
        print(f"S3 put_object_tagging response: {tag_response}")

        print(f"Telegram messages saved to S3 with category: {category}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Telegram messages fetched and saved to S3 with category"})
        }
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def extract_url(entities):
    """
    Extract URL from the message entities.

    Parameters:
    entities (list): List of entities in the message.

    Returns:
    str: Extracted URL or empty string if not found.
    """
    # Iterate through entities to find the URL
    for entity in entities:
        if entity.get('_') == 'MessageEntityTextUrl':
            return entity.get('url', '')
    return ''  # Return empty string if URL not found    

async def fetch_telegram_messages(client, channels):
    """
    Fetch messages from specified Telegram channels.

    Parameters:
    client (TelegramClient): Instance of the Telegram client.
    channels (list): List of channel identifiers or URLs.

    Returns:
    list: List of messages fetched from Telegram.
    """
    await client.start()  # Start the client

    print("Client Created")

    all_messages = []

    # Calculate the timestamp for 1 day ago (adjusted from original 2 days)
    one_day_ago = datetime.now() - timedelta(days=1)
    one_day_ago_timestamp = int(one_day_ago.timestamp())

    for user_input_channel in channels:
        if user_input_channel.isdigit():
            entity = PeerChannel(int(user_input_channel))
        else:
            entity = user_input_channel

        my_channel = await client.get_entity(entity)

        offset_id = 0
        limit = 100
        
        total_count_limit = 0

        # Set the search query to an empty string (to match all messages)
        search_query = ""

        # Set the message filter to match all messages (InputMessagesFilterEmpty)
        message_filter = InputMessagesFilterEmpty()

        # Fetch messages until there are no more messages
        while True:
            print("Current Offset ID:", offset_id, "; Total Messages:", len(all_messages))
            history = await client(SearchRequest(
                peer=my_channel,
                q=search_query,
                filter=message_filter,
                min_date=one_day_ago_timestamp,
                max_date=datetime.now(), 
                offset_id=offset_id,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                if "\n\nTo comment, follow this link" in message.message:
                    # Remove the unwanted sentence
                    message.message = message.message.replace("\n\nTo comment, follow this link", "")
                
                # Append the original message dictionary to all_messages
                all_messages.append(message.to_dict())

            offset_id = messages[-1].id  # Simplified from: messages[len(messages) - 1].id
            if total_count_limit != 0 and len(all_messages) >= total_count_limit:
                break

    # Disconnect the client after fetching messages
    await client.disconnect()  

    return all_messages

# Invoke the lambda handler if running as a script
if __name__ == "__main__":
    lambda_handler(None, None)
