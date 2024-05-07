import json
import boto3
from datetime import datetime
import requests
from datetime import datetime, timedelta
import configparser

# AWS S3 client
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Read configuration
config = configparser.ConfigParser()
config.read("config.ini")

# Define the base URL for the GDELT API
gdelt_api_url = config['GDELT']['api_url'] 

def lambda_handler(event, context):
    try:
        # Read category from the event
        category = event['category']
        print(f"Category '{category}' read from the event.")

        # Fetch GDELT articles based on category
        gdelt_data = make_gdelt_request(config, category)
        if gdelt_data is None:
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Failed to fetch GDELT articles."})
            }
        print("GDELT articles fetched based on the category.")

        article_list = extract_articles(gdelt_data['articles'])

        # Convert article list to JSON
        json_data = json.dumps(article_list)
        print("Article list converted to JSON.")

        # Invoke destination Lambda function
        invoke_destination_lambda(json_data, category)
        print("Destination Lambda function invoked.")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"{category.capitalize()} GDELT articles fetched and sent to destination Lambda"})
        }
    except KeyError as e:
        print("KeyError:", str(e))
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "KeyError occurred.", "error": str(e)})
        }
    except Exception as e:
        print("Exception:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error.", "error": str(e)})
        }

def make_gdelt_request(config, category):
    try:
        # Access the keyword bank based on the category
        keyword_query = config['GDELT'][category]  
        print("Keyword query accessed based on the category.")

        params = {
            'format': 'JSON',
            'timespan': '24H',
            'query': f'{keyword_query} sourcelang:eng',
            'mode': 'artlist',
            'maxrecords': 80,
            'sort': 'hybridrel'
        }

        url = gdelt_api_url + '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None
    except Exception as e:
        print("Error in make_gdelt_request:", str(e))
        return None
        
def extract_articles(articles):
    try:
        extracted_articles = []
        today_date = datetime.now().strftime('%Y-%m-%d')
        for article in articles:
            article_title = article['title']
            article_url = article['url']
            article_date = datetime.strptime(article['seendate'], "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M")
            article_date_format=datetime.strptime(article['seendate'], "%Y%m%dT%H%M%SZ").strftime('%Y-%m-%d')
            if article_date_format == today_date:
                extracted_article = {
                        "title": article_title,
                        "date": article_date,
                        "url": article_url,
                }
                extracted_articles.append(extracted_article)

        print("Articles extracted.")
        return extracted_articles
    except Exception as e:
        print("Error in extract_articles:", str(e))
        return []

def invoke_destination_lambda(json_data, category):
    try:
        # Invoke the destination Lambda function
        lambda_client.invoke(
            FunctionName='summarize_articles_gdelt',
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps({'category': category, 'json_data': json_data})
        )
        print("Destination Lambda function invoked.")
    except Exception as e:
        print("Error in invoke_destination_lambda:", str(e))