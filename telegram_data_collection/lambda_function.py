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
        
        # Reading Configs
        category = event['category']
        config = configparser.ConfigParser()
        config.read("config.ini")
        bucket_name = config['S3']['bucket_name']
        
        # Create the TelegramClient using the StringSession
        client = TelegramClient(StringSession(string_session), api_id, api_hash)

        # Running the asyncio loop
        all_messages = asyncio.get_event_loop().run_until_complete(fetch_telegram_messages(client))

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

        # Adjust the file name to include the UUID
        file_name = f'telegram_messages_{str(uuid.uuid4())}.json'

        
        # Upload the JSON data to the S3 bucket with the adjusted file name
        upload_byte_stream = bytes(json_data.encode('UTF-8'))
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=upload_byte_stream)

        # Add tags to the uploaded S3 object
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
    # Iterate through entities to find the URL
    for entity in entities:
        if entity.get('_') == 'MessageEntityTextUrl':
            return entity.get('url', '')
    return ''  # Return empty string if URL not found    


async def fetch_telegram_messages(client):
    await client.start()  # Start the client

    print("Client Created")

    user_input_channel = 'https://t.me/englishabuali'

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    all_messages = []

    # Calculate the timestamp for 2 days ago
    two_days_ago = datetime.now() - timedelta(days=1)
    two_days_ago_timestamp = int(two_days_ago.timestamp())

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
            min_date=two_days_ago_timestamp,
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