import json
import boto3
from simphile import jaccard_similarity
import Levenshtein
from datetime import datetime, timedelta
import uuid
import traceback


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
                    "message": message1["message"],
                    "url": message1["url"],
                    "date": message1["date"],
                    "source": source
                }],
                "location": message1["location"],
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
                                "message": message2["message"],
                                "url": message2["url"],
                                "date": message2["date"],
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
    
# Function to check if the list contains a dictionary with Key 'Corellation_flag'
def no_has_corellation_flag(objects):
    for obj in objects:
        if obj.get('Key') == 'Corellation_flag':
            return False
    return True

def lambda_handler(event, context):
    try:
        # S3 bucket name
        input_bucket_name = "classified-data-geoshield"
        output_bucket_name = "maching-events-geoshield"
        
        # Extracting category from the event 
        print("event: " + str(event))
        file_name = event['Records'][0]['s3']['object']['key']
        
        # Initialize Boto3 client for S3
        s3 = boto3.client('s3')
        
        # Read JSON data from input S3 bucket
        tags = s3.get_object_tagging(Bucket=input_bucket_name, Key=file_name)
        
        # Accessing the 'Category' value from the 'TagSet'
        category = None
        for tag in tags['TagSet']:
            if tag['Key'] == 'Category':
                category = tag['Value']
                break

        print("Category:", category) 
        
        if not category:
            print("error: Category tag not found in the S3 object")
            return {
                'statusCode': 400,
                'body': json.dumps({"error": "Category tag not found in the S3 object"})
            }
        
        # List all objects in the bucket
        response = s3.list_objects_v2(Bucket=input_bucket_name)

        # Filter objects based on the 'Category' tag
        filtered_objects = []
        today_date = datetime.now().strftime('%Y-%m-%d')
        for obj in response.get('Contents', []):
            obj_key = obj['Key']
            obj_tags = s3.get_object_tagging(Bucket='classified-data-geoshield', Key=obj_key)['TagSet']
            for tag in obj_tags:
                if tag['Key'] == 'Category' and tag['Value'] == category:
                    last_modified = obj['LastModified'].strftime('%Y-%m-%d')
                    if last_modified == today_date:
                        if no_has_corellation_flag(obj_tags):
                            filtered_objects.append(obj_key)
                            print("filtered_objects.append "+ obj_key)
                    break

        # Ensure at least two files are found for the specified category and date
        if len(filtered_objects) < 2:
            print("Less than two files found for the specified category, date and corellation flag")
            return {
                'statusCode': 400,
                'body': json.dumps({"error": "Less than two files found for the specified category, date and corellation flag"})
            }

        # Sort files by last modified date in descending order
        filtered_objects.sort(key=lambda x: s3.head_object(Bucket='classified-data-geoshield', Key=x)['LastModified'], reverse=True)

        # Select the newest two files
        newest_files = filtered_objects[:2]

        # Load JSON files from S3
        gdelt_file_key = newest_files[0]
        telegram_file_key = newest_files[1]
        print("gdelt_file_key: " + gdelt_file_key)
        print("telegram_file_key: " + telegram_file_key)
        gdelt_messages = load_json_from_s3(input_bucket_name, gdelt_file_key)
        telegram_messages = load_json_from_s3(input_bucket_name, telegram_file_key)

        # Combine all messages for comparison
        all_messages = telegram_messages + gdelt_messages
        
        # Generate message buckets
        message_buckets = generate_message_buckets(all_messages)
        
        # Check if message_buckets is empty
        if not message_buckets:
            print("No similar messages found to be corellated")
            return {
                'statusCode': 200,
                'body': json.dumps('No similar messages found to be corellated')
            }
            
        # Filter out buckets with count == 0
        filtered_buckets = {k: v for k, v in message_buckets.items() if v["count"] > 0}    

        print("message buckets:" + str(message_buckets))
        # Calculate final_score for buckets with count > 0
        for bucket in filtered_buckets.values():  # Use filtered_buckets instead of message_buckets
            avg_score = bucket["average_score"]
            count = bucket["count"] / 10
            bucket["final_score"] = (avg_score * 0.7) + (count * 0.3)
        
        today = datetime.now().strftime('%Y-%m-%d')

        file_exists_today = False
        matching_category = None
        
        # List all files in the S3 bucket
        response = s3.list_objects_v2(Bucket=output_bucket_name)
        
        # Check if any file matches today's date and category
        for obj in response.get('Contents', []):
            file_key = obj['Key']
            last_modified = obj['LastModified'].strftime('%Y-%m-%d')
            last_modified_with_time = obj['LastModified'].strftime('%Y-%m-%d %H:%M')  # Printing last modified date with current hour and minute
            print("Last modified date with time for file", file_key + ":", last_modified_with_time)
            # Check if the file's last modified date matches today's date
            if today == last_modified:
                response_tags = s3.get_object_tagging(Bucket=output_bucket_name, Key=file_key)
                print("found file: "+ file_key)
                for tag in response_tags['TagSet']:
                    # Check if the file's category matches the event's category
                    if tag['Key'] == 'Category':
                        if tag['Value'] == category:
                            file_exists_today = True
                            break
            if file_exists_today:
                break
        
        # If a matching file exists, append new information to it
        if file_exists_today:

            # Upload the updated content back to S3
            s3.put_object(Bucket=output_bucket_name, Key=file_key, Body=json.dumps(filtered_buckets))
            
            # Add category tag to the file in S3
            s3.put_object_tagging(
                Bucket=output_bucket_name,
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
            print(f"Exists {file_key} save successfully!")
            
        else:
            file_name = f'matching_messages_{str(uuid.uuid4())}.json'
            # Proceed with regular process of saving processed messages as a new file in S3
            s3.put_object(Bucket=output_bucket_name, Key=file_name, Body=json.dumps(filtered_buckets))
            # Add category tag to the file in S3
            s3.put_object_tagging(
                Bucket=output_bucket_name,
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
        
        
        # ADD 'Corellation_flag' tag from files
        s3.put_object_tagging(
            Bucket=input_bucket_name,
            Key=telegram_file_key,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'Category',
                        'Value': category
                    },
                    {
                        'Key': 'Corellation_flag',
                        'Value': 'True'
                    }
                ]
            }
        )
        
        s3.put_object_tagging(
            Bucket=input_bucket_name,
            Key=gdelt_file_key,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'Category',
                        'Value': category
                    },
                    {
                        'Key': 'Corellation_flag',
                        'Value': 'True'
                    }
                ]
            }
        )

        print("Matching messages processed successfully and stored in maching-events-geoshield bucket")
        return {
            'statusCode': 200,
            'body': json.dumps('Matching messages processed successfully and stored in maching-events-geoshield bucket')
        }
    except Exception as e:
        print("Error: " + str(e))
        traceback.print_exc()  
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }
