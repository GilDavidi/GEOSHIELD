import json
import boto3
from concurrent.futures import ThreadPoolExecutor
from spacy.lang.en import English

# Load English tokenizer
nlp = English()

def extract_locations(file_name, bucket_name, messages, output_bucket_name, output_key, batch_size=100):
    # Initialize Boto3 clients for S3 and Comprehend
    s3 = boto3.client('s3')
    comprehend = boto3.client('comprehend')
    
    # Define function to perform Comprehend analysis for a batch of messages
    def process_batch(batch):
        classified_messages = []
        for message in batch:
            text = message['message'].strip()  # Remove leading/trailing whitespace
            if text:  # Check if the message is not empty
                # Perform entity recognition
                entities_response = comprehend.detect_entities(Text=text, LanguageCode='en')
                entities = entities_response['Entities']
                # Extract locations mentioned in the text
                locations = [entity['Text'] for entity in entities if entity['Type'] == 'LOCATION']
                # Extract primary location (for simplicity, just take the first location mention)
                primary_location = locations[0] if locations else None
                # Add location to the existing fields in the message
                message['location'] = primary_location
                classified_messages.append(message)
        return classified_messages
    
    # Split messages into batches
    batches = [messages[i:i+batch_size] for i in range(0, len(messages), batch_size)]
    
    # Process batches concurrently using a ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        classified_results = list(executor.map(process_batch, batches))
    
    # Flatten the list of classified results
    classified_messages = [msg for batch_result in classified_results for msg in batch_result]
    
    # Convert classified messages to JSON string
    locations_json = json.dumps(classified_messages)
    


    
    return classified_messages
    
    

def lambda_handler(event, context):
    try:
        file_name = event['file_name']
        print("extract location " + file_name + " start")
        result = extract_locations(
                file_name=file_name,    
                bucket_name='classified-data-geoshield',
                messages= event['messages'],
                output_bucket_name='classified-data-geoshield',
                output_key='telegram_classified_messages.json'
            )
        # Invoke another Lambda function to process the results
        lambda_client = boto3.client('lambda')
        invoke_response = lambda_client.invoke(
            FunctionName='data_extract_events',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "file_name": file_name,
                "messages": result
            })
        )

        print("extract location " + file_name + " end")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Successfully sent to data_extract_events"})
        }

    except Exception as e:
        print("error "+ str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
