import json
import boto3
from datetime import datetime
import requests
import configparser
import traceback

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
        category = event['category']
        print(f"Category '{category}' read from the event.")
        # Check if custom_uuid exists in the event
        domains = None
        if 'custom_uuid' in event and event['custom_uuid']:
            custom_uuid = event['custom_uuid']
            print(f"Custom UUID: {custom_uuid}")

            # Access the correct file in the S3 bucket
            file_name = f'channels_config_{custom_uuid}.json'
            obj = s3.get_object(Bucket='s3-files-geoshield', Key=file_name)
            file_content = obj['Body'].read().decode('utf-8')
            config_data = json.loads(file_content)
            domains = list(config_data.get("GDELT_Domains", {}).values())
            category = config_data.get("category", "default")

        # Fetch GDELT articles based on category or loaded domains
        gdelt_data = make_gdelt_request(config, category, domains)
        if gdelt_data is None:
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Failed to fetch GDELT articles."})
            }
        print("GDELT articles fetched based on the category or loaded domains.")

        article_list = extract_articles(gdelt_data)
        
        # Convert article list to JSON
        json_data = json.dumps(article_list)
        print("Article list converted to JSON.")

        # Invoke destination Lambda function with custom_uuid if exists
        invoke_destination_lambda(json_data, category, custom_uuid if 'custom_uuid' in event else None)
        print("Destination Lambda function invoked.")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"{category.capitalize()} GDELT articles fetched and sent to destination Lambda"})
        }
    except KeyError as e:
        print("KeyError:", str(e))
        traceback.print_exc()  
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "KeyError occurred.", "error": str(e)})
        }
    except Exception as e:
        print("Exception:", str(e))
        traceback.print_exc()  
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error.", "error": str(e)})
        }

def make_gdelt_request(config, category, domains=None):
    try:
        query_list = json.loads(config['GDELT'][category])
        combined_articles = []
        seen_urls = set()  # Set to track seen URLs

        for query in query_list:
            query_terms = query
            
            if domains:
                # Construct the domain query terms
                domain_query = ' OR '.join([f"domain:{domain} OR domainis:{domain}" for domain in domains])
                # Append the domain query terms to the main query
                query_terms += ' and (' + domain_query + ')'
            
            params = {
                'format': 'JSON',
                'timespan': '24H',
                'query': f'{query_terms} sourcelang:eng',  # Construct the full query
                'mode': 'artlist',
                'maxrecords': 250,
                'sort': 'hybridrel'
            }

            # Construct the full request URL
            url = gdelt_api_url + '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
            
            # Print the full request URL for debugging purposes
            print("Full request URL:", url)

            # Make the request
            response = requests.get(url)

            if response.status_code == 200:
                gdelt_data = response.json()
                for article in gdelt_data.get('articles', []):
                    article_url = article['url']
                    if article_url not in seen_urls:
                        combined_articles.append(article)
                        seen_urls.add(article_url)
            else:
                print(f"Error: {response.status_code}")
                return None
        
        return combined_articles
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
            article_domain = article['domain']
            article_date_format = datetime.strptime(article['seendate'], "%Y%m%dT%H%M%SZ").strftime('%Y-%m-%d')
            if article_date_format == today_date:
                extracted_article = {
                    "title": article_title,
                    "date": article_date,
                    "url": article_url,
                    "domain": article_domain
                }
                extracted_articles.append(extracted_article)

        print("Articles extracted.")
        return extracted_articles
    except Exception as e:
        print("Error in extract_articles:", str(e))
        return []

def invoke_destination_lambda(json_data, category, custom_uuid=None):
    try:
        # Prepare payload for the destination Lambda function
        payload = {
            'category': category,
            'json_data': json_data
        }
        if custom_uuid:
            payload['custom_uuid'] = custom_uuid

        # Invoke the destination Lambda function
        lambda_client.invoke(
            FunctionName='summarize_articles_gdelt',
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(payload)
        )
        print("Destination Lambda function invoked.")
    except Exception as e:
        print("Error in invoke_destination_lambda:", str(e))
