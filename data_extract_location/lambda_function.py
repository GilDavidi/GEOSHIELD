import json
import concurrent.futures
import boto3
from g4f.client import Client

def generate_text(message, question):
    client = Client()
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": question + message}]
    )
    return response.choices[0].message.content

def process_message(message):
    text_to_check = "\u6d41\u91cf\u5f02\u5e38" 
    if message["message"]:
        location = None
        
        location_question = "Based on the information provided in the text, please identify the primary location where the main event is taking place. Choose only one location that accurately represents the central focus of the event (only answer like: Tel Aviv). If you cannot determine a specific location, please indicate null."
        
        # Define maximum number of attempts to find a valid location
        max_attempts = 3
        attempt_count = 0
        
        while attempt_count < max_attempts:
            location = generate_text(message["message"], location_question)
            
            # Check if the location is meaningful or empty
            if location is not None and location.strip():  # Ensure location is not empty or whitespace
                message["location"] = str(location)
                break  # Exit loop if a valid location is found
            else:
                attempt_count += 1  # Increment attempt count
                
        # If no valid location is found after maximum attempts, default to 'null'
        if attempt_count == max_attempts:
            message["location"] = "null"
            
    return message


def lambda_handler(event, context):
    try:
        file_name = event['file_name']
        print("extract location " + file_name + " start")
        
        # Process messages
        messages = event['messages']
        max_workers = 10
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_messages = list(executor.map(process_message, messages))
            
        # Invoke another Lambda function to process the results
        lambda_client = boto3.client('lambda')
        invoke_response = lambda_client.invoke(
            FunctionName='data_extract_events',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "file_name": file_name,
                "messages": processed_messages
            })
        )
        
        print("extract location " + file_name + " end")
        
        return {
            "statusCode": 200,
            "body": json.dumps({"messages": processed_messages})
        }

    except Exception as e:
        print("error "+ str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
