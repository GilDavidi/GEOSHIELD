from newspaper import Article
import nltk
import json
import boto3
import uuid 
import requests

nltk.data.path.append("/tmp")
nltk.download("punkt", download_dir="/tmp")


# AWS S3 client
s3 = boto3.client('s3')

def get_article_text(url):
    try:
        response = requests.get(url, timeout=5)  # Set a timeout for the request
        if response.status_code == 200:
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            return article.summary
        return None
    except Exception as e:
        print("An error occurred:", e)
        return None  # Return None if summary extraction fails

def lambda_handler(event, context):
    try:
        # Extract the JSON data from the event
        json_data = json.loads(event['json_data'])

        # Initialize a list to store article summaries
        summaries = []

        # Iterate through each article in the JSON data
        for article_data in json_data:
            title = article_data['title']
            date = article_data['date']
            url = article_data['url']
            
            # Get the summary of the article
            summary = get_article_text(url)
            
            unique_id = str(uuid.uuid4())  

            # If summary extraction was successful, add article info to summaries list
            if summary:
                article_info = {
                    'id': unique_id,
                    'title': title,
                    'date': date,
                    'url': url,
                    'message': summary
                }
                summaries.append(article_info)

        # Convert the list of article summaries to JSON format
        summaries_json = json.dumps(summaries)

        # Upload the JSON data to the S3 bucket
        bucket_name = 'raw-data-geoshield'
        s3.put_object(Bucket=bucket_name, Key='gdelt_articles.json', Body=summaries_json)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Article summaries saved to S3"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }