import requests
from bs4 import BeautifulSoup

def get_text_from_url(url):
    try:
        # Make a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract text from the parsed HTML
            text = soup.get_text(separator='\n', strip=True)

            return text
        else:
            print(f"Error: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Example usage
url = "https://thediplomat.com/2024/01/two-malaysian-men-sentenced-to-23-years-prison-for-bali-bombing/"
webpage_text = get_text_from_url(url)

if webpage_text:
    print(webpage_text)
