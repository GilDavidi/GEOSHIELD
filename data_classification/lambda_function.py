import boto3
import json
import requests
import configparser

lambda_client = boto3.client('lambda')

# Read configuration
config = configparser.ConfigParser()
config.read("config.ini")

def classify_and_invoke(messages, file_name, category, bucket_name):
    try:
        # Define the endpoint of the model service
        model_endpoint = config['EC2']['URL']

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

                # Check if predicted category matches the file category and score is above 0.4
                if predicted_category == category and score is not None and score >= 0.4:
                    # Add classification and score to the original message
                    message['classification'] = predicted_category
                    message['score'] = score
                else:
                    # If predicted category does not match or score is under 0.6, skip inserting it to the message
                    message['classification'] = None
                    message['score'] = None

        # Prepare payload for the next Lambda function
        payload = {
            'file_name': file_name,
            'category': category,
            'bucket_name': bucket_name,
            'messages': [msg for msg in messages if msg.get('classification') is not None]
        }

        # Invoke the destination Lambda function
        lambda_client.invoke(
            FunctionName='data_extract_events',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

        print("Successfully invoked data_extract_events")
        return {"message": "Successfully invoked data_extract_events"}
    except Exception as e:
        print("Error in classify_and_invoke:", e)
        return {"error": str(e)}

def lambda_handler(event, context):
    try:
        # Extracting object key and messages from the event
        file_name = event['file_name']
        messages = event['messages']
        category = event['category']
        bucket_name = event['bucket_name']
        print("New " + file_name + " upload")

        # Classify messages and invoke data_extract_location
        result = classify_and_invoke(
            messages,
            file_name,
            category,  # Pass the category from the event
            bucket_name
        )

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }
    except Exception as e:
        print("Error in lambda_handler:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }