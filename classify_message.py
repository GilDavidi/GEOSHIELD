import json
import re
import sys
import os

def classify_security_issue(text, security_keywords):
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

def process_messages(all_messages, security_keywords):
    # Check for security issues and classify messages
    classified_messages = []

    for message in all_messages:
        text = message.get('message', '')
        # Convert text to lowercase
        text_lower = text.lower()
        # Classify security issue and detect keywords
        classification, detected_keywords = classify_security_issue(text_lower, security_keywords)
        # Remove duplicate keywords
        unique_keywords = list(set(detected_keywords))
        classified_messages.append({
            'id': message['id'],
            'classification': classification,
            'detected_keywords': unique_keywords
        })

    return classified_messages

def main(json_file_path, keywords_file_path):
    with open(json_file_path, 'r') as json_file:
        all_messages = json.load(json_file)

    with open(keywords_file_path, 'r') as keywords_file:
        security_keywords = json.load(keywords_file)

    classified_messages = process_messages(all_messages, security_keywords)

    # Get the base name of the JSON file without the extension
    base_name = os.path.splitext(os.path.basename(json_file_path))[0]
    output_file_path = f"classified_messages_{base_name}.json"

    with open(output_file_path, 'w') as classified_file:
        json.dump(classified_messages, classified_file, indent=2)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python classify_message.py <json_file_path>")
        sys.exit(1)

    json_file_path = sys.argv[1]  # Get JSON file path from command-line argument
    keywords_file_path = 'security_keywords.json'  # Path to your JSON file containing security keywords
    main(json_file_path, keywords_file_path)
