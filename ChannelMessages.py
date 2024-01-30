import configparser
import json
import asyncio
import re
import spacy
nlp = spacy.load('en_core_web_sm')
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)

# Function to classify security issues based on keywords
def classify_security_issue(text):
    # Define keywords related to security issues
    terrorism_keywords = ['terrorism', 'terrorist', 'attack', 'bombing']
    antisemitism_keywords = ['antisemitism', 'anti-semitism', 'hate speech']
    war_keywords = ['war', 'conflict', 'battle', 'armed','strikes','strike']

    # Combine all keywords into a single pattern
    security_pattern = '|'.join(map(re.escape, terrorism_keywords + antisemitism_keywords + war_keywords))

    # Case-insensitive pattern matching
    pattern = re.compile(security_pattern, re.IGNORECASE)

    # Check if the text contains any security-related keywords
    if pattern.search(text):
        return 'Security Issue'
    else:
        return 'No Security Issue'


# some functions to parse json date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# Setting configuration values
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']

api_hash = str(api_hash)

phone = config['Telegram']['phone']
username = config['Telegram']['username']

# Create the client and connect
client = TelegramClient(username, api_id, api_hash)

async def main(phone):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    user_input_channel = input('enter telegram URL:')

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    # Calculate the datetime for 48 hours ago
    time_threshold = datetime.now() - timedelta(hours=48)

    offset_id = 0
    limit = 1
    all_messages = []
    total_messages = 0
    total_count_limit = 0

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=0,
            offset_date=int(time_threshold.timestamp()),  # Set the timestamp for 48 hours ago
            add_offset=0,
            limit=limit,
            max_id=offset_id,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            # Create a copy of the original message dictionary
            message_dict = message.to_dict().copy()

            # Check if the "media" key exists and remove it
            if 'media' in message_dict:
                del message_dict['media']

            # Append the modified message dictionary to all_messages
            all_messages.append(message_dict)

        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    with open('channel_messages.json', 'w') as outfile:
        json.dump(all_messages, outfile, cls=DateTimeEncoder)


    # Check locations using Geopy and store in a dictionary
    locations_dict = {}
    i = 0
    for message in all_messages:
        sample_text = message.get('message', '')
        doc = nlp(sample_text)

        for entity in doc.ents:
            if entity.label_ == 'GPE':
                print(entity.text)
        print('//////////')
        i = i+1
        print(i)

    # Check for security issues and classify messages
    classified_messages = []
    for message in all_messages:
        text = message.get('message', '')
        # print(text)
        is_security_issue = classify_security_issue(text)
        classified_messages.append({
            'id': message['id'],
            'classification': is_security_issue
        })

    # Save classified messages to a JSON file
    with open('classified_messages.json', 'w') as classified_file:
        json.dump(classified_messages, classified_file)

with client:
    client.loop.run_until_complete(main(phone))
