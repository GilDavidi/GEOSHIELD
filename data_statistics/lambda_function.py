import json
import boto3
import googlemaps
from shapely.geometry import Point
import geopandas as gpd
from collections import defaultdict
import pandas as pd
import traceback

# Initialize the S3 client
s3 = boto3.client('s3')

def get_google_maps_key():
    """
    Retrieve the Google Maps API key from AWS Secrets Manager.

    Returns:
        str: The Google Maps API key.

    Raises:
        ValueError: If the Google Maps API key is not found in the secrets.
        Exception: For other errors retrieving the secret.
    """
    secret_name = "google_api_secrets"
    region_name = "eu-west-1"
    
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager', region_name=region_name)
    
    try:
        # Retrieve the secret
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        google_maps_key = secret.get('google_key')
        
        if not google_maps_key:
            raise ValueError("Google Maps API key not found in the secrets")
        
        return google_maps_key
    
    except Exception as e:
        print(f"Error retrieving Google Maps API key: {e}")
        raise

# Initialize the Google Maps client with the retrieved API key
google_maps_key = get_google_maps_key()
gmaps = googlemaps.Client(key=google_maps_key)

def get_country_polygon(country_name):
    """
    Retrieve the polygon for a given country from the GeoPandas dataset.

    Args:
        country_name (str): The name of the country to retrieve the polygon for.

    Returns:
        shapely.geometry.Polygon: The polygon representing the country.

    Raises:
        ValueError: If the country is not found in the dataset.
    """
    print(f"Fetching polygon for country: {country_name}")
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
    matching_countries = world[world.name.str.contains(country_name.strip(), case=False, na=False)]
    
    if matching_countries.empty:
        raise ValueError(f"Country '{country_name}' not found in the dataset")
    
    country_polygon = matching_countries.geometry.values[0]
    print(f"Found polygon for country: {country_name}")
    return country_polygon

def get_point(location):
    """
    Geocode a location to get its latitude and longitude.

    Args:
        location (str): The location to geocode.

    Returns:
        shapely.geometry.Point: The point representing the geocoded location, or None if geocoding fails.
    """
    print(f"Geocoding location: {location}")
    geocode_result = gmaps.geocode(location)
    
    if geocode_result:
        loc = geocode_result[0]['geometry']['location']
        point = Point(loc['lng'], loc['lat'])
        print(f"Geocoded location '{location}' to point: {point}")
        return point
    else:
        print(f"Failed to geocode location: {location}")
        return None

def is_physically_contained(country_polygon, sub_location):
    """
    Check if a given location is within a country polygon.

    Args:
        country_polygon (shapely.geometry.Polygon): The polygon representing the country.
        sub_location (str): The location to check.

    Returns:
        bool: True if the location is within the country polygon, False otherwise.
    """
    print(f"Checking if location '{sub_location}' is within the country")
    sub_location_point = get_point(sub_location)

    if country_polygon and sub_location_point:
        contained = country_polygon.contains(sub_location_point)
        print(f"Location '{sub_location}' is within the country: {contained}")
        return contained
    print(f"Could not determine containment for location '{sub_location}'")
    return False

def process_json_files(bucket_name, location, country_polygon):
    """
    Process JSON files in an S3 bucket to count the number of events per category and date.

    Args:
        bucket_name (str): The name of the S3 bucket containing the JSON files.
        location (str): The location to filter the events by.
        country_polygon (shapely.geometry.Polygon): The polygon representing the country.

    Returns:
        dict: A dictionary with event categories as keys and counts per date as values.
    """
    print(f"Processing JSON files for location: {location} in bucket: {bucket_name}")
    result = defaultdict(lambda: defaultdict(int))

    objects = s3.list_objects_v2(Bucket=bucket_name)

    for obj in objects.get('Contents', []):
        print(f"Processing file: {obj['Key']}")
        file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
        file_data = json.load(file_obj['Body'])

        for message in file_data:
            message_location = message['location']
            message_category = message['classification']
            message_date = pd.to_datetime(message['date']).date()

            if is_physically_contained(country_polygon, message_location):
                result[message_category][message_date] += 1
                print(f"Message '{message}' is contained in '{location}', updating result")

    print(f"Finished processing files for location: {location}")
    return result

def check_file_exists(bucket_name, location):
    """
    Check if a file for the given location exists in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        location (str): The location to check.

    Returns:
        tuple: A tuple containing a boolean indicating if the file exists and the file name if it does.
    """
    print(f"Checking if file exists for location '{location}' in bucket '{bucket_name}'")
    objects = s3.list_objects_v2(Bucket=bucket_name)
    for obj in objects.get('Contents', []):
        if location.strip().lower() in obj['Key'].lower():
            print(f"File found for location '{location}': {obj['Key']}")
            return True, obj['Key']
    print(f"No file found for location '{location}'")
    return False, None

def lambda_handler(event, context):
    """
    AWS Lambda function handler that processes a request for location-based statistics.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The context object passed to the Lambda function.

    Returns:
        dict: The response containing the status code, body, and headers.
    """
    bucket_name = 'classified-data-geoshield'
    output_bucket_name = 'statistics-geoshield'
    
    location = event['queryStringParameters'].get('location')
    
    if not location:
        print("Location parameter is missing")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Location parameter is required'}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }

    print(f"Received request for location: {location}")
    file_exists, output_file_name = check_file_exists(output_bucket_name, location)

    if file_exists:
        print(f"File already exists for location: {location}. Reading from S3.")
        file_obj = s3.get_object(Bucket=output_bucket_name, Key=output_file_name)
        file_content = json.load(file_obj['Body'])
        return {
            'statusCode': 200,
            'body': json.dumps(file_content),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }
    else:
        print(f"No existing file for location: {location}. Starting processing.")
        country_polygon = get_country_polygon(location)
        data = process_json_files(bucket_name, location, country_polygon)

        result = {location: []}
        for category, dates in data.items():
            sorted_dates = sorted(dates.items())
            category_data = {
                category: [{'date': date.strftime('%Y-%m-%d'), 'count': count} for date, count in sorted_dates]
            }
            result[location].append(category_data)

        output_file_name = f"{location}_statistics.json"
        print(f"Saving processed data to S3: {output_file_name}")
        s3.put_object(Bucket=output_bucket_name, Key=output_file_name, Body=json.dumps(result))

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'There is no information yet about this country. The process will take about 10 minutes.'
            }),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            }
        }

# Example event for testing
event = {
    'queryStringParameters': {
        'location': 'France'
    }
}

# Test the lambda function
if __name__ == "__main__":
    print(lambda_handler(event, None))
