from newspaper import Article
import nltk
import json
import boto3
import uuid  as uuid_module
import requests


# Download nltk data to /tmp
nltk.data.path.append("/tmp")
nltk.download("punkt", download_dir="/tmp")

# AWS S3 client
s3 = boto3.client('s3')

# Load domains from JSON file
def load_domains(filename):
    with open(filename, 'r') as file:
        return json.load(file)

# Classify domain
def classify_domain(domain, domain_data):
    if domain in domain_data["international"]:
        return "International"
    for region, domains in domain_data["local"].items():
        if domain in domains:
            return "Local - " + region
    return "Unknown"

def get_article_text(url):
    try:
        print("Downloading article from:", url)
        response = requests.get(url, timeout=5)  # Set a timeout for the request
        if response.status_code == 200:
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            return article.summary
        else:
            print("Failed to download article. Status code:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred while downloading article:", e)
        return None  # Return None if summary extraction fails

def lambda_handler(event, context):
    try:
        # Load domain data
        domain_data = load_domains("news_domains.json")
        
        # Extract the category and JSON data from the event
        category = event['category']
        json_data = json.loads(event['json_data'])
        custom_uuid = event.get('custom_uuid')  # Check if custom_uuid exists

        print("Processing articles for category:", category)

        # Initialize a list to store article summaries
        summaries = []

        # Iterate through each article in the JSON data
        for article_data in json_data:
            title = article_data['title']
            date = article_data['date']
            url = article_data['url']
            domain = article_data['domain']

            print("Processing article:", title)
            
            # Classify the domain
            domain_classification = classify_domain(domain, domain_data)

            # Get the summary of the article
            summary = get_article_text(url)
            
            unique_id = str(uuid_module.uuid4())
                   
            # If summary extraction was successful, add article info to summaries list
            if summary:
                article_info = {
                    'id': unique_id,
                    'title': title,
                    'date': date,
                    'url': url,
                    'domain': domain,
                    'domain_classification': domain_classification,
                    'message': summary
                }
                summaries.append(article_info)
            else:
                print("Failed to get summary for article:", title)

        # Convert the list of article summaries to JSON format
        summaries_json = json.dumps(summaries)

        # Adjust the file name to include the category and custom_uuid if exists
        file_name = f'gdelt_articles_{str(uuid_module.uuid4())}.json' if not custom_uuid else f'gdelt_articles_{custom_uuid}.json'

        print("Uploading data to S3...")

        # Define the bucket name based on the presence of custom_uuid
        bucket_name = 'raw-data-geoshield' if not custom_uuid else 'custom-raw-data-geoshield'

        # Upload the JSON data to the S3 bucket with the adjusted file name
        tags = [{'Key': 'Category', 'Value': category}]
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=summaries_json, Tagging='&'.join(['{}={}'.format(tag['Key'], tag['Value']) for tag in tags]))
        
        print("Data uploaded successfully.")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Article summaries saved to S3"})
        }
    except Exception as e:
        print("An error occurred:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
