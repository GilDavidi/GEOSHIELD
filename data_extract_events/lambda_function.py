import json
import boto3
import concurrent.futures
from g4f.client import Client
from datetime import datetime

def generate_text(message, question):
    client = Client()
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": question + message}]
    )
    return response.choices[0].message.content

def process_message(message):
    text_to_check = "\u6d41\u91cf\u5f02\u5e38" 
    unwanted_text = "该ip请求过多已被暂时限流 过两分钟再试试吧(目前限制了每小时60次 正常人完全够用,学校网络和公司网络等同网络下共用额度,如果限制了可以尝试切换网络使用 ),本网站正版地址是 https://chat18.aichatos.xyz 如果你在其他网站遇到此报错，请访问https://chat18.aichatos8.xyz ，如果你已经在本网站，请关闭代理，不要使用公共网络访问,如需购买独立次数请访问 https://binjie09.shop"
    
    event_breakdown_question="What are the events in the text? Write it only in English, in the form of a list of reports (without conclusions and without some summary sentence after the events or title) (For me, an event is an event that can be placed on a map - that is, with a location). If several events are related to each other, list them as a single event in brief"
    # Define maximum number of attempts to find a valid location
    max_attempts = 3
    attempt_count = 0
    event_breakdown = None
    while attempt_count < max_attempts:
        event_breakdown = generate_text(message["message"], event_breakdown_question)
        if event_breakdown is not None and "January 2022" not in event_breakdown and text_to_check not in event_breakdown and unwanted_text not in event_breakdown:
            message["event_breakdown"] = str(event_breakdown)
            break  # Exit loop if a valid location is found
        else:
            attempt_count += 1  # Increment attempt count
            
        # If no valid location is found after maximum attempts, default to 'null'
        if attempt_count == max_attempts:
            message["event_breakdown"] = "null"
    
    return message
    
def compare_first_two_words(file1, file2):
    # Split the file names by underscore
    words1 = file1.split('_')
    words2 = file2.split('_')
    
    # Compare the first two words
    return words1[0] == words2[0] and words1[1] == words2[1]
    
    
def merge_json(json1, json2):
    merged_data = {}
    for item in json1:
        message = item['message']
        if message not in merged_data:
            merged_data[message] = item
    for item in json2:
        message = item['message']
        if message not in merged_data:
            merged_data[message] = item

    merged_list = list(merged_data.values())
    return merged_list

def lambda_handler(event, context):
    try:
        s3 = boto3.client('s3')
        bucket_name = 'classified-data-geoshield'
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
            processed_messages = list(executor.map(process_message, messages))
            
            
            
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
            updated_messages = merge_json(existing_messages,processed_messages)

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
        
        return {
            'statusCode': 200,
            'body': json.dumps('Files processed and saved successfully!')
        }
    except Exception as e:
        print("Error: " + str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }