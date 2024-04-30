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

def find_matching_messages(messages):
    matching_messages = []

    message_indices = {i: message.get("id") for i, message in enumerate(messages)}

    for i, message1 in enumerate(messages):
        for j in range(i+1, len(messages)):  # Start from i+1 to avoid duplicate comparisons
            message2 = messages[j]
                
            if message1.get("classification") == "security" and message2.get("classification") == "security":
                message1_timestamp = datetime.fromisoformat(message1.get("date"))
                message2_timestamp = datetime.fromisoformat(message2.get("date"))
                time_difference = abs(message1_timestamp - message2_timestamp)

                # Check if the time difference is within 6 hours
                if time_difference <= timedelta(hours=6):
                    gdelt_location = message2.get("location")
                    text1 = message1.get("message")
                    text2 = message2.get("message")
                    levenshtein_sim = levenshtein_similarity(text1 , text2)
                    jaccard_sim = jaccard_similarity(text1 , text2)
            
                    # Decide if there is a match based on the similarity thresholds
                    if jaccard_sim > 0.6 and levenshtein_sim > 0.6:
                        matching_messages.append({
                            "message1_id": message_indices[i],
                            "message2_id": message_indices[j],
                            "message1": text1,
                            "message2": text2,
                            "jaccard_similarity": jaccard_sim,
                            "levenshtein_similarity": levenshtein_sim,
                            "location": gdelt_location
                        })

    return matching_messages

def remove_duplicates(matching_messages):
    # Create a dictionary to store the best match for each message
    best_matches = {}

    for message in matching_messages:
        message1_id = message["message1_id"]
        if message1_id not in best_matches or message["jaccard_similarity"] > best_matches[message1_id].get("jaccard_similarity", 0):
            best_matches[message1_id] = message

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
    gdelt_messages = load_json_from_s3(input_bucket_name, gdelt_file_key)

    matching_messages = find_matching_messages(gdelt_messages)
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

