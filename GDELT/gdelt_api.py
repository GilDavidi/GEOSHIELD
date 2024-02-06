import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import os
import json


# Assuming the current script is in a folder (current_folder) and the JSON file is in the parent folder
current_folder = os.path.dirname(__file__)
parent_folder = os.path.abspath(os.path.join(current_folder, '..'))

# Specify the JSON file name
json_file_name = 'security_keywords.json'

# Construct the full path to the JSON file
keywords_file_path = os.path.join(parent_folder, json_file_name)

# Define the base URL for the GDELT API
gdelt_api_url = "https://api.gdeltproject.org/api/v2/doc/doc"

# Define the keyword banks
security_keywords = [
    'terrorism',
    'terrorist',
    'attack' ,
    'bombing'
]

# Get the current time and the time 72 hours ago
current_time = datetime.utcnow()
time_72_hours_ago = current_time - timedelta(hours=72)

# Format the time strings for the API query
start_time = time_72_hours_ago.strftime("%Y%m%d%H%M%S")
end_time = current_time.strftime("%Y%m%d%H%M%S")

# Function to make API request
def make_gdelt_request(keywords):
    # Join the keywords with spaces for the API query
    keyword_query = ' '.join(keywords)

    # Define other parameters for the API query
    params = {
        'format': 'JSON',
        'timespan': '72H',
        'query': f'{keyword_query} sourcelang:eng',
        'mode': 'artlist',
        'sort': 'hybridrel'
    }

    # Print the URL before making the request
    url = gdelt_api_url + '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
    print("API Request URL:", url)

    # Make the API request
    response = requests.get(gdelt_api_url, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Get the HTML content from the response
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract and print details of each article
        articles = soup.find_all('div', class_='gkg_article')
        for idx, article in enumerate(articles, 1):
            print(f"\nArticle {idx} Details:")
            print("Title:", article.find('div', class_='gkg_title').get_text(strip=True))
            print("URL:", article.find('div', class_='gkg_url').get_text(strip=True))
            print("Summary:", article.find('div', class_='gkg_summary').get_text(strip=True))

            # Visit the article URL and extract text content
            article_url = article.find('div', class_='gkg_url').get_text(strip=True)
            article_text = get_article_text(article_url)
            print("Text:", article_text)
            # Classify security issues for GDELT articles
            classification, detected_keywords = classify_security_issue(article_text, keywords_file_path)

        return html_content
    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code}")
        return None

# Function to visit article URL and extract text content
def get_article_text(url):
    response = requests.get(url)
    if response.status_code == 200:
        article_soup = BeautifulSoup(response.text, 'html.parser')
        # Extract text content and clean unnecessary spaces and line breaks
        article_text = ' '.join(article_soup.stripped_strings)
        return article_text
    else:
        print(f"Error fetching article content from {url}")
        return "N/A"

# Function to save articles to a JSON file
def save_articles_to_json(article_list, source):
    output_articles = []
    for article in article_list:
        article_title = article['title']
        article_url = article['url']
        article_date = datetime.strptime(article['seendate'], "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%S+00:00")
        article_text = get_article_text(article_url)

        article_dict = {
            "title": article_title,
            "date": article_date,
            "url": article_url,
            "source": source,
            "text": article_text
        }

        output_articles.append(article_dict)

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

# Perform the request to GDELT API
terrorism_json_string = make_gdelt_request(security_keywords)
terrorism_json = json.loads(terrorism_json_string)

# Extract articles from the JSON response
article_list = save_articles_to_json(terrorism_json['articles'], source="GDELT")

# Classify GDELT articles for security issues
gdelt_classified_messages = classify_gdelt_articles(article_list, keywords_file_path)

# Save GDELT classified messages to a JSON file
with open('classified_messages.json', 'w') as classified_file:
    json.dump(gdelt_classified_messages, classified_file, indent=2)
