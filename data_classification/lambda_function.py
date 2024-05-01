import boto3
import json
import requests
import configparser
lambda_client = boto3.client('lambda')
# Read configuration
config = configparser.ConfigParser()
config.read("config.ini")

def classify_and_invoke(bucket_name, input_key, file_name):
    try:
        # Initialize Boto3 client for S3
        s3 = boto3.client('s3')
        
        # Read JSON data from input S3 bucket
        response = s3.get_object(Bucket=bucket_name, Key=input_key)
        json_data = response['Body'].read().decode('utf-8')
        messages = json.loads(json_data)
        
        # Define the endpoint of model service
        model_endpoint =  config['EC2']['URL'] 
        
        # Iterate through each message and classify it
        for message in messages:
            # Extract the message text
            text = message.get('message', '').strip()
            
            # If the message is not empty, send it to the model for classification
            if text:
                # Prepare the payload for the model
                payload = {"text": text}
                
                # Send the request to the model service
                response = requests.post(model_endpoint, json=payload)
                
                # Process the model's response
                if response.status_code == 200:
                    model_output = response.json()
                    
                    # Extract Score and Predicted category from the model's output
                    predicted_category = model_output.get("Predicted", None)
                    score = model_output.get("Score", None)
                    
                    # Add classification and score to the original message
                    message['classification'] = predicted_category
                    message['score'] = score
        
        # Prepare payload for next Lambda function
        payload = {
            'file_name': file_name,
            'messages': messages
        }
        
        # Invoke the destination Lambda function
        lambda_client.invoke(
            FunctionName='data_extract_location',
            InvocationType='Event',  
            Payload=json.dumps(payload)
        )
        
        return {"message": "Successfully invoked data_extract_location"}
    except Exception as e:
        print("error: "+ str(e))
        return str(e)

def lambda_handler(event, context):
    try:
        # Extracting S3 bucket name and object key from the SQS message
        message_body = json.loads(event['Records'][0]['body'])
        inner_message_body = json.loads(message_body['Message'])
        file_name = inner_message_body['Records'][0]['s3']['object']['key']
    
        print("new " + file_name + " upload")
        
        # Classify messages and invoke data_extract_location
        result = classify_and_invoke(
            bucket_name='raw-data-geoshield',
            input_key=file_name,
            file_name=file_name
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }
    except Exception as e:
        print("error: "+ str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
