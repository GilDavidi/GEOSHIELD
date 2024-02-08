import requests
from datetime import datetime, timedelta
from html_parser import get_article_text
import re
import json


# Define the base URL for the GDELT API
gdelt_api_url = "https://api.gdeltproject.org/api/v2/doc/doc"

# Define the keyword banks
security_keywords = [
    'killed',
    'strike',
    'bombing',
    'attack',
    'terrorism',
    'terrorist',
    'war',
    'battle'
]

# Get the current time and the time 72 hours ago
current_time = datetime.utcnow()
time_72_hours_ago = current_time - timedelta(hours=72)

# Format the time strings for the API query
start_time = time_72_hours_ago.strftime("%Y%m%d%H%M%S")
end_time = current_time.strftime("%Y%m%d%H%M%S")

# Function to make API request
def make_gdelt_request(keywords):
    keyword_query = ' or '.join(keywords)

    # Define other parameters for the API query
    params = {
        'format': 'JSON',
        'timespan': '24H',
        'query': f'({keyword_query})  sourcelang:eng',
        'mode': 'artlist',
        'maxrecords': 75,
        'sort': 'hybridrel'
    }

    # Construct the full URL
    url = gdelt_api_url + '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
    print("API Request URL:", url)

    # Make the API request
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Return JSON content from the response
        return response.json()
    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code}")
        return None


# Function to save articles to a JSON file
def save_articles_to_json(article_list, source):
    output_articles = []
    article_id = 1  # Initialize article id

    for article in article_list:
        article_title = article['title']
        article_url = article['url']
        article_date = datetime.strptime(article['seendate'], "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%S+00:00")
        article_text = get_article_text(article_url)

        article_dict = {
            "id": article_id,
            "title": article_title,
            "date": article_date,
            "url": article_url,
            "source": source,
            "message": article_text
        }

        output_articles.append(article_dict)
        article_id += 1  # Increment article id

    with open('gdelt_articles.json', 'w') as json_file:
        json.dump(output_articles, json_file, indent=2)
    print("Articles saved to gdelt_articles.json")
    return output_articles

def classify_security_issue(text, keywords_file_path):
    with open(keywords_file_path, 'r') as keywords_file:
        security_keywords = json.load(keywords_file)

    # Combine all keywords into a single pattern
    security_pattern = '|'.join(map(re.escape, security_keywords))

    # Case-insensitive pattern matching
    pattern = re.compile(security_pattern, re.IGNORECASE)

    # Find all keywords in the text
    detected_keywords = pattern.findall(text)

    # Check if the text contains any security-related keywords
    if detected_keywords:
        return 'Security Issue', detected_keywords
    else:
        return 'No Security Issue', []

# Function to classify security issues for GDELT articles
def classify_gdelt_articles(article_list, keywords_file_path):
    classified_messages = []

    for idx, article in enumerate(article_list):
        # Extract details from GDELT article based on HTML structure
        article_title = article.get('title', '')  # Replace with the actual key for title
        article_url = article.get('url', '')  # Replace with the actual key for URL
        article_text = get_article_text(article_url)  # Assuming get_article_text is defined

        # Classify security issues for GDELT articles
        classification, detected_keywords = classify_security_issue(article_text, keywords_file_path)

        classified_messages.append({
            'id': article_title,  # Use title instead of ID
            'classification': classification,
            'detected_keywords': detected_keywords
        })

    return classified_messages


terrorism_json = make_gdelt_request(security_keywords)

# Extract articles from the JSON response
article_list = save_articles_to_json(terrorism_json['articles'], source="GDELT")


