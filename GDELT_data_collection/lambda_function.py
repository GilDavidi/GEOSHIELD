import json
import boto3
from datetime import datetime
import requests
import configparser
from newspaper import Article

# AWS S3 client
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Define the base URL for the GDELT API
gdelt_api_url = "https://api.gdeltproject.org/api/v2/doc/doc"

def lambda_handler(event, context):
    try:
        # Read category from the event
        category = event['category']

        # Read configuration
        config = configparser.ConfigParser()
        config.read("config.ini")

        # Fetch GDELT articles based on category
        gdelt_data = make_gdelt_request(config, category)
        article_list = extract_articles(gdelt_data['articles'])

        # Convert article list to JSON
        json_data = json.dumps(article_list)
        
        # Invoke destination Lambda function
        invoke_destination_lambda(json_data)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"{category.capitalize()} GDELT articles fetched and sent to destination Lambda"})
        }
    except KeyError as e:
        return {
            "errorMessage": str(e),
            "errorType": type(e)._name_
        }

def make_gdelt_request(config, category):
    keyword_bank = config['GDELT'][category]  # Access the keyword bank based on the category
    keyword_query = ' OR '.join(keyword_bank.split(','))

    params = {
        'format': 'JSON',
        'timespan': '48H',
        'query': f'({keyword_query}) sourcelang:eng',
        'mode': 'artlist',
        'maxrecords': 80,
        'sort': 'hybridrel'
    }

    url = gdelt_api_url + '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
    print(url)
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def extract_articles(articles):
    extracted_articles = []

    for article in articles:
        article_title = article['title']
        article_url = article['url']
        article_date = datetime.strptime(article['seendate'], "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M")
        extracted_article = {
                "title": article_title,
                "date": article_date,
                "url": article_url,}
        extracted_articles.append(extracted_article)

    return extracted_articles

def invoke_destination_lambda(json_data):
    # Invoke the destination Lambda function
    lambda_client.invoke(
        FunctionName='summarize_articles_gdelt',
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps({'json_data': json_data})
    )