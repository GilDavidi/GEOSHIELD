import json
import boto3
import uuid
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

def lambda_handler(event, context):
    try:
        s3 = boto3.client('s3')
        bucket_name = 'classified-data-geoshield'
        
        # Extract file name and messages from the event payload
        file_name = event['file_name']
        messages = event['messages']
        
        print("Extracting data from " + file_name + " started")
        
        # Define the maximum number of workers for concurrent processing
        max_workers = 10
        
        # Process messages concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_messages = list(executor.map(process_message, messages))
        
        # Generate a UUID for the output key
        output_uuid = str(uuid.uuid4())
        
        # Save updated messages to S3 with UUID appended to the filename
        output_key = f"{file_name.split('.')[0]}_{output_uuid}.json"
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=json.dumps(processed_messages))
        
        print("Data from " + file_name + " processed and saved successfully!")
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
