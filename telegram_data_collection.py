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

# Lambda handler function
def lambda_handler(event, context):

    # Reading Configs
    config = configparser.ConfigParser()
    config.read("config.ini")
    api_id = config['Telegram']['api_id']
    api_hash = config['Telegram']['api_hash']
    string_session = config['Telegram']['string_session']
    bucket_name = config['S3']['bucket_name']
    
    # Create the TelegramClient using the StringSession
    client = TelegramClient(StringSession(string_session), api_id, api_hash)

    # Running the asyncio loop
    all_messages = asyncio.get_event_loop().run_until_complete(fetch_telegram_messages(client))

    print("Lambda handler execution completed.")

    # Create a list to hold selected messages
    selected_messages = []

    # Iterate through all messages and extract required fields
    for message in all_messages:
        # Exclude empty messages
        if message.get('message'):
            # Generate a unique ID for each message
            unique_id = str(uuid.uuid4())  
            selected_message = {
                "id": unique_id,  # Replace the original ID with the unique ID
                "channel_id": message['peer_id']['channel_id'] if 'peer_id' in message and 'channel_id' in message['peer_id'] else None,
                "date": message['date'].isoformat(),
                "message": message['message']
            }
            selected_messages.append(selected_message)

    # Convert the list of selected messages to JSON
    json_data = json.dumps(selected_messages)

    # Upload the JSON data to the S3 bucket
    upload_byte_stream = bytes(json_data.encode('UTF-8'))
    s3.put_object(Bucket=bucket_name, Key='telegram_messages.json', Body=upload_byte_stream)

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Telegram messages fetched and saved to S3"})
    }


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
    two_days_ago = datetime.now() - timedelta(days=2)
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
