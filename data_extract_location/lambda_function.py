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
    unwanted_text = "该ip请求过多已被暂时限流 过两分钟再试试吧(目前限制了每小时60次 正常人完全够用,学校网络和公司网络等同网络下共用额度,如果限制了可以尝试切换网络使用 ),本网站正版地址是 https://chat18.aichatos.xyz 如果你在其他网站遇到此报错，请访问https://chat18.aichatos8.xyz ，如果你已经在本网站，请关闭代理，不要使用公共网络访问,如需购买独立次数请访问 https://binjie09.shop"
    
    if message["message"]:
        print("Processing message:", message["message"])
        
        location = None
        
        location_question = "Based on the information in the text, please identify the main location where the main event takes place. Choose only one location that accurately represents the main focus of the event. Be sure to find the most specific location (eg city and state and not just a country and so on). (Answer in English Only the location you found. Answer for example: 'ODESA', note that there may not be a central event that can be placed on a map - if you cannot determine a specific location, answer only the exact word- null without any additions to the word"     
        # Define maximum number of attempts to find a valid location
        max_attempts = 3
        attempt_count = 0
        
        while attempt_count < max_attempts:
            location = generate_text(message["message"], location_question)
            
            # Check if the location is meaningful, not empty, and does not contain specific words
            if location is not None and "January 2022" not in location and text_to_check not in location and unwanted_text not in location and 'null' not in location:
                message["location"] = str(location)
                print("Location found:", location)
                break  # Exit loop if a valid location is found
            else:
                attempt_count += 1  # Increment attempt count
                
        # If no valid location is found after maximum attempts, default to 'null'
        if attempt_count == max_attempts:
            message["location"] = "null"
            print("No valid location found for the message. Message dropped.")
            
    return message


def lambda_handler(event, context):
    try:
        # Extracting S3 bucket name and object key from the SQS message
        message_body = json.loads(event['Records'][0]['body'])
        inner_message_body = json.loads(message_body['Message'])
        file_name = inner_message_body['Records'][0]['s3']['object']['key']
        
        print("Extracting location for " + file_name + " started")
        
        # Initialize Boto3 client for S3
        s3 = boto3.client('s3')
        
        # Read JSON data from input S3 bucket
        response = s3.get_object(Bucket='raw-data-geoshield', Key=file_name)
        tags=s3.get_object_tagging(Bucket='raw-data-geoshield', Key=file_name)
        # Accessing the 'Category' value from the 'TagSet'
        for tag in tags['TagSet']:
            if tag['Key'] == 'Category':
                category = tag['Value']
                break

        print("Category:", category) 
        
        json_data = response['Body'].read().decode('utf-8')
        messages = json.loads(json_data)
        
        max_workers = 10
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_messages = list(filter(lambda x: x is not None, executor.map(process_message, messages)))
            
        # Filter out messages with null location
        processed_messages = [msg for msg in processed_messages if msg["location"] != "null"]
        

        # Invoke another Lambda function to process the results
        lambda_client = boto3.client('lambda')
        invoke_response = lambda_client.invoke(
            FunctionName='data_classification',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "file_name": file_name,
                "messages": processed_messages,
                "category": category  # Pass the category tag to the next Lambda function
            })
        )
        
        print("Extracting location for " + file_name + " completed")
        
        return {
            "statusCode": 200,
            "body": json.dumps({"messages": processed_messages})
        }

    except Exception as e:
        print("Error: " + str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }