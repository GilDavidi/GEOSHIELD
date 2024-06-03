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
    # Load the dataset of country boundaries from a direct source
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
    matching_countries = world[world.name.str.contains(country_name, case=False, na=False)]
    
    if matching_countries.empty:
        raise ValueError(f"Country '{country_name}' not found in the dataset")
    
    country_polygon = matching_countries.geometry.values[0]
    
    return country_polygon

def get_point(location):
    geocode_result = gmaps.geocode(location)
    
    if geocode_result:
        loc = geocode_result[0]['geometry']['location']
        return Point(loc['lng'], loc['lat'])
    else:
        return None

def is_physically_contained(country_name, sub_location):
    country_polygon = get_country_polygon(country_name)
    sub_location_point = get_point(sub_location)

    if country_polygon and sub_location_point:
        return country_polygon.contains(sub_location_point)
    return False

def process_json_files(bucket_name, location):
    result = defaultdict(lambda: defaultdict(int))
    country_polygon = get_country_polygon(location)

    # List objects in the bucket
    objects = s3.list_objects_v2(Bucket=bucket_name)

    for obj in objects.get('Contents', []):
        file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
        file_data = json.load(file_obj['Body'])

        for message in file_data:
            message_location = message['location']
            message_category = message['classification']
            message_date = pd.to_datetime(message['date']).date()

            if is_physically_contained(location, message_location):
                result[message_category][message_date] += 1

    return result

def lambda_handler(event, context):
    bucket_name = 'classified-data-geoshield'
    location = event['location']
    output_bucket_name = 'statistics-geoshield'
    output_file_name = f"{location}_statistics.json"

    data = process_json_files(bucket_name, location)

    # Convert result to the desired format
    result = {location: []}
    for category, dates in data.items():
        sorted_dates = sorted(dates.items())
        category_data = {
            category: [{'date': date.strftime('%Y-%m-%d'), 'count': count} for date, count in sorted_dates]
        }
        result[location].append(category_data)

    # Save the result to the output bucket
    s3.put_object(Bucket=output_bucket_name, Key=output_file_name, Body=json.dumps(result))

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }

# Example event for testing
event = {
    'location': 'USA'
}

# Test the lambda function
if __name__ == "__main__":
    print(lambda_handler(event, None))
