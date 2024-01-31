import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import json

# Define the base URL for the GDELT API
gdelt_api_url = "https://api.gdeltproject.org/api/v2/doc/doc"

# Define the keyword banks
security_keywords = [
    "haifa",
    "attack",
    "soldier"
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
        'format': 'html',
        'timespan': '72H',
        'query': f'{keyword_query} sourcelang:eng',
        'mode': 'artlist',
        'maxrecords': '3',  # Reduce to 3 for demonstration purposes
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
def save_articles_to_json(articles, source):
    article_list = []
    for i, article in enumerate(articles):
        title = article.select_one('span.arttitle').text.strip()

        # Extract date using the script content
        date_script = soup.find('script', string=re.compile(r'sourceinfo_date_' + str(i)))
        date_match = re.search(r"'(\d{2}/\d{2}/\d{4} \d{2}:\d{2} UTC)'", date_script.string)
        date_str = date_match.group(1) if date_match else "N/A"

        # Convert the date to the desired format
        article_date = datetime.strptime(date_str, "%m/%d/%Y %H:%M UTC").strftime("%Y-%m-%dT%H:%M:%S+00:00")

        # Extract the URL from the 'A' tag outside the 'div'
        article_url = article['href'] if article['href'] else "N/A"

        # Get the text content of the article
        article_text = get_article_text(article_url)

        # Build the article dictionary
        article_dict = {
            "title": title,
            "date": article_date,
            "url": article_url,
            "source": source,
            "text": article_text
        }

        # Append the article dictionary to the list
        article_list.append(article_dict)

    # Save the article list to a JSON file
    with open('gdelt_articles.json', 'w') as json_file:
        json.dump(article_list, json_file, indent=2)
    print("Articles saved to gdelt_articles.json")

# Perform three separate requests for each word bank
terrorism_html = make_gdelt_request(security_keywords)

# Extract articles from the HTML content
html_content = terrorism_html
soup = BeautifulSoup(html_content, 'html.parser')
articles = soup.select('div[id^="maincontent"] > table > a')

# Save articles to JSON file
save_articles_to_json(articles, source="GDELT")
