import json
import boto3
from g4f.client import Client

def generate_event_breakdown(text):
    client = Client()
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "What are the events in the text? Write it only in English ,in the form of a list of reports (without conclusions and without some summary sentence after the events or title) (For me, an event is an event that can be placed on a map - that is, with a location). If several events are related to each other, list them as a single event in brief " + text}]
    )
    return response.choices[0].message.content

def process_messages(messages):
    text_to_check = "\u6d41\u91cf\u5f02\u5e38" 
    for message in messages:
        if message["message"]:
            event_breakdown = None
            while not event_breakdown or event_breakdown.startswith(text_to_check):
                event_breakdown = generate_event_breakdown(message["message"])
            if event_breakdown is not None:
                message["event_breakdown"] = str(event_breakdown)

def lambda_handler(event, context):
    try:
        s3 = boto3.client('s3')
        bucket_name = 'classified-data-geoshield'
        
        # Extract file name and messages from the event payload
        file_name = event['file_name']
        messages = event['messages']
        
        print("extract " + file_name + " start")
        
        # Process messages
        process_messages(messages)
        
        # Save updated messages to S3
        output_key = f"{file_name.split('.')[0]}_classified.json"
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=json.dumps(messages))
        
        print("classified " + file_name + " processed and saved successfully!")
        return {
            'statusCode': 200,
            'body': json.dumps('Files processed and saved successfully!')
        }
    except Exception as e:
        print("error "+ str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }