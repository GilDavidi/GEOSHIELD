import json
import boto3
from simphile import jaccard_similarity
import Levenshtein
from datetime import datetime, timedelta

def load_json_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def levenshtein_similarity(text1, text2):
    return 1 - Levenshtein.distance(text1, text2) / max(len(text1), len(text2))

def find_matching_messages(telegram_messages, gdelt_messages):
    matching_messages = []

    for telegram_message in telegram_messages:
        for gdelt_message in gdelt_messages:
            if telegram_message.get("classification") == "security" and gdelt_message.get("classification") == "security":
                telegram_timestamp = datetime.fromisoformat(telegram_message.get("date"))
                gdelt_timestamp = datetime.fromisoformat(gdelt_message.get("date"))
                time_difference = abs(telegram_timestamp - gdelt_timestamp)

                # Check if the time difference is within 6 hours
                if time_difference <= timedelta(hours=6):
                    telegram_text = telegram_message.get("message")
                    telegram_location = telegram_message.get("location")
                    gdelt_text = gdelt_message.get("message")
                    levenshtein_sim = levenshtein_similarity(telegram_text , gdelt_text)
                    jaccard_sim = jaccard_similarity(telegram_text , gdelt_text)
                    
                    # Decide if there is a match based on some threshold
                    if jaccard_sim > 0.15 and levenshtein_sim > 0.25:
                        matching_messages.append({
                            "Telegram_id": telegram_message.get('id'),
                            "GDELT_id": gdelt_message.get('id'),
                            "Telegram_message": telegram_text,
                            "GDELT_message": gdelt_text,
                            "jaccard_similarity": jaccard_sim,
                            "levenshtein_similarity": levenshtein_sim,
                            "location": telegram_location
                        })

    return matching_messages

def remove_duplicates(matching_messages):
    # Create a dictionary to store the best match for each message
    best_matches = {}

    for message in matching_messages:
        telegram_id = message["Telegram_id"]
        if telegram_id not in best_matches or message["jaccard_similarity"] > best_matches[telegram_id].get("jaccard_similarity", 0):
            best_matches[telegram_id] = message

    return list(best_matches.values())

def upload_to_s3(bucket_name, file_key, file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, file_key)

def lambda_handler(event, context):
    # S3 bucket name and file keys
    input_bucket_name = "classified-data-geoshield"
    output_bucket_name = "maching-events-geoshield"
    telegram_file_key = "telegram_messages_classified.json"
    gdelt_file_key = "gdelt_articles_classified.json"
    
    # Load JSON files from S3
    telegram_messages = load_json_from_s3(input_bucket_name, telegram_file_key)
    gdelt_messages = load_json_from_s3(input_bucket_name, gdelt_file_key)

    matching_messages = find_matching_messages(telegram_messages, gdelt_messages)
    unique_matching_messages = remove_duplicates(matching_messages)

    # Export matching messages to a temporary file
    output_file_path = "/tmp/matching_messages.json"
    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(unique_matching_messages, file, indent=4)

    # Upload the output JSON file to the output bucket
    upload_to_s3(output_bucket_name, "matching_messages.json", output_file_path)

    return {
        'statusCode': 200,
        'body': json.dumps('Matching messages processed successfully and stored in maching-events-geoshield bucket')
    }

