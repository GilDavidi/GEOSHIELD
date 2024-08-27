from newspaper import Article
import nltk
import json
import boto3
import uuid as uuid_module
import requests

# Download NLTK data to /tmp to make it available for Lambda
nltk.data.path.append("/tmp")
nltk.download("punkt", download_dir="/tmp")

# AWS S3 client
s3 = boto3.client('s3')

def load_domains(filename):
    """Load domain classifications from a JSON file."""
    with open(filename, 'r') as file:
        return json.load(file)

def classify_domain(domain, domain_data):
    """Classify the domain as 'International' or 'Local' based on the loaded data."""
    if domain in domain_data.get("international", []):
        return "International"
    for region, domains in domain_data.get("local", {}).items():
        if domain in domains:
            return f"Local - {region}"
    return "Unknown"

def get_article_text(url):
    """Fetch and extract the summary of the article from the given URL."""
    try:
        print(f"Downloading article from: {url}")
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            return article.summary
        else:
            print(f"Failed to download article. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred while downloading the article: {e}")
        return None

def lambda_handler(event, context):
    try:
        # Load domain data from the JSON file
        domain_data = load_domains("news_domains.json")
        
        # Extract the category and JSON data from the event
        category = event.get('category', 'Unknown')
        json_data = json.loads(event.get('json_data', '{}'))
        custom_uuid = event.get('custom_uuid')

        print(f"Processing articles for category: {category}")

        # Store article summaries
        summaries = []

        # Iterate through each article in the JSON data
        for article_data in json_data:
            title = article_data.get('title')
            date = article_data.get('date')
            url = article_data.get('url')
            domain = article_data.get('domain')

            print(f"Processing article: {title}")
            
            # Classify the domain
            domain_classification = classify_domain(domain, domain_data)

            # Get the summary of the article
            summary = get_article_text(url)
            unique_id = str(uuid_module.uuid4())
            
            # If summary extraction was successful, add the article info
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
                print(f"Failed to get summary for article: {title}")

        # Convert the list of article summaries to JSON format
        summaries_json = json.dumps(summaries)

        # Adjust the file name based on the presence of custom_uuid
        file_name = f'gdelt_articles_{custom_uuid}.json' if custom_uuid else f'gdelt_articles_{str(uuid_module.uuid4())}.json'
        
        # Determine the bucket name based on whether custom_uuid exists
        bucket_name = 'custom-raw-data-geoshield' if custom_uuid else 'raw-data-geoshield'

        print(f"Uploading data to S3 bucket: {bucket_name} with file name: {file_name}")

        # Upload the JSON data to S3 and tag with the category
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=summaries_json,
            Tagging=f"Category={category}"
        )

        print("Data uploaded successfully.")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Article summaries saved to S3"})
        }
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
