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

def generate_message_buckets(messages, similarity_threshold=0.6):
    message_buckets = {}  # Dictionary to store similar messages
    assigned_ids = set()  # Set to keep track of IDs already assigned to a group

    for i, message1 in enumerate(messages):
        if message1["id"] not in assigned_ids:
            # Initialize bucket with the message itself
            source = "Telegram" if "channel_id" in message1 else "GDELT"
            message_buckets[message1["id"]] = {
                "messages": [{
                    "id": message1["id"],
                    "source": source
                }],
                "total_score": 0,
                "count": 0
            }
            assigned_ids.add(message1["id"])  # Add the current message ID to assigned IDs

            for j, message2 in enumerate(messages):
                if i != j and message2["id"] not in assigned_ids:  # Skip comparison if i == j or if message2 already assigned to a group
                    text1 = message1["message"]
                    text2 = message2["message"]
                    jaccard_sim = jaccard_similarity(text1, text2)
                    levenshtein_sim = levenshtein_similarity(text1, text2)

                    if jaccard_sim > similarity_threshold and levenshtein_sim > similarity_threshold:
                        # Check if message2 already assigned to a group
                        assigned_to_group = False
                        for bucket in message_buckets.values():
                            if message2["id"] in [msg["id"] for msg in bucket["messages"]]:
                                assigned_to_group = True
                                break

                        if not assigned_to_group:
                            source = "Telegram" if "channel_id" in message2 else "GDELT"
                            message_buckets[message1["id"]]["messages"].append({
                                "id": message2["id"],
                                "source": source
                            })
                            message_buckets[message1["id"]]["total_score"] += (jaccard_sim + levenshtein_sim) / 2
                            message_buckets[message1["id"]]["count"] += 1

                            # Add message2 ID to assigned_ids
                            assigned_ids.add(message2["id"])

    # Calculate average score for each bucket
    for bucket in message_buckets.values():
        if bucket["count"] > 0:
            bucket["average_score"] = bucket["total_score"] / bucket["count"]
        else:
            bucket["average_score"] = 0

    return message_buckets


def upload_to_s3(bucket_name, file_key, file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, file_key)

def lambda_handler(event, context):
    # S3 bucket name
    input_bucket_name = "classified-data-geoshield"
    output_bucket_name = "maching-events-geoshield"
    
    # List objects in the bucket
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=input_bucket_name)
    
    # Extract file keys and timestamps
    file_timestamps = {}
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        timestamp = obj['LastModified']
        file_timestamps[file_key] = timestamp
    
    # Sort files by timestamp in descending order
    sorted_files = sorted(file_timestamps.items(), key=lambda x: x[1], reverse=True)
    
    # Select the newest two files
    newest_files = sorted_files[:2]
    
    # Load JSON files from S3
    gdelt_file_key = newest_files[0][0]
    telegram_file_key = newest_files[1][0]
    gdelt_messages = load_json_from_s3(input_bucket_name, gdelt_file_key)
    telegram_messages = load_json_from_s3(input_bucket_name, telegram_file_key)

    # Combine all messages for comparison
    all_messages = telegram_messages + gdelt_messages
    
    # Generate message buckets
    message_buckets = generate_message_buckets(all_messages)

    # Calculate final_score for each bucket
    final_scores = []
    for bucket in message_buckets.values():
        avg_score = bucket["average_score"]
        count = bucket["count"]
        final_score = (avg_score * 0.7) + (count * 0.3)
        final_scores.append(final_score)

    # Calculate min and max final scores
    min_score = min(final_scores)
    max_score = max(final_scores)

    # Normalize final scores between 0 and 1
    for bucket in message_buckets.values():
        final_score = (bucket["average_score"] * 0.7) + (bucket["count"] * 0.3)
        normalized_score = (final_score - min_score) / (max_score - min_score)
        bucket["final_score"] = normalized_score

    # Filter out buckets with count == 0
    filtered_buckets = {k: v for k, v in message_buckets.items() if v["count"] > 0}

    # Export matching messages to a temporary file
    output_file_path = "/tmp/matching_messages.json"
    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(filtered_buckets, file, indent=4)

    # Upload the output JSON file to the output bucket
    upload_to_s3(output_bucket_name, "matching_messages.json", output_file_path)

    return {
        'statusCode': 200,
        'body': json.dumps('Matching messages processed successfully and stored in maching-events-geoshield bucket')
    }

