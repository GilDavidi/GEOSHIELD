import boto3
import json
import googlemaps
from shapely.geometry import Point
import geopandas as gpd
from collections import defaultdict
import pandas as pd

# Initialize the Google Maps client
gmaps = googlemaps.Client(key='AIzaSyDdKQY_n89HWZDY7032fvrra6JrECnFAjU')

# Initialize the S3 client
s3 = boto3.client('s3')

def get_country_polygon(country_name):
    print(f"Fetching polygon for country: {country_name}")
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
    matching_countries = world[world.name.str.contains(country_name.strip(), case=False, na=False)]
    
    if matching_countries.empty:
        raise ValueError(f"Country '{country_name}' not found in the dataset")
    
    country_polygon = matching_countries.geometry.values[0]
    print(f"Found polygon for country: {country_name}")
    return country_polygon

def get_point(location):
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

def is_physically_contained(country_name, sub_location):
    print(f"Checking if location '{sub_location}' is within country '{country_name}'")
    country_polygon = get_country_polygon(country_name)
    sub_location_point = get_point(sub_location)

    if country_polygon and sub_location_point:
        contained = country_polygon.contains(sub_location_point)
        print(f"Location '{sub_location}' is within country '{country_name}': {contained}")
        return contained
    print(f"Could not determine containment for location '{sub_location}' in country '{country_name}'")
    return False

def process_json_files(bucket_name, location):
    print(f"Processing JSON files for location: {location} in bucket: {bucket_name}")
    result = defaultdict(lambda: defaultdict(int))
    country_polygon = get_country_polygon(location)

    objects = s3.list_objects_v2(Bucket=bucket_name)

    for obj in objects.get('Contents', []):
        print(f"Processing file: {obj['Key']}")
        file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
        file_data = json.load(file_obj['Body'])

        for message in file_data:
            message_location = message['location']
            message_category = message['classification']
            message_date = pd.to_datetime(message['date']).date()

            if is_physically_contained(location, message_location):
                result[message_category][message_date] += 1
                print(f"Message '{message}' is contained in '{location}', updating result")

    print(f"Finished processing files for location: {location}")
    return result

def check_file_exists(bucket_name, location):
    print(f"Checking if file exists for location '{location}' in bucket '{bucket_name}'")
    objects = s3.list_objects_v2(Bucket=bucket_name)
    for obj in objects.get('Contents', []):
        if location.strip().lower() in obj['Key'].lower():
            print(f"File found for location '{location}': {obj['Key']}")
            return True, obj['Key']
    print(f"No file found for location '{location}'")
    return False, None

def lambda_handler(event, context):
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
        data = process_json_files(bucket_name, location)

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
