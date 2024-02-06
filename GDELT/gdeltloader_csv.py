import os
import glob
import requests
from io import BytesIO
import zipfile
import pandas as pd

# Function to download and extract GDELT files
def download_and_extract(url, output_folder):
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            zip_file.extractall(output_folder)
        print(f"Download and extraction successful for: {url}")
    else:
        print(f"Failed to download and extract: {url}")

# Specify the GDELT master file list URL
gdelt_masterfile_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

# Make a GET request to the GDELT master file list URL
response = requests.get(gdelt_masterfile_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Get the last 60 lines of the text file
    lines = response.text.strip().split('\n')[-60:]

    # Extract the URLs from the last 60 lines
    zip_urls = [line.split(' ')[-1] for line in lines]

    # Specify the folder for extracted files
    extraction_folder = "extracted_files"

    # Download and extract the last 60 zip files
    for zip_url in zip_urls:
        download_and_extract(zip_url, extraction_folder)

    ####################################################################################################

    # Continue with the code to merge and organize exported CSV files

    # Specify the path where your exported CSV files are located
    path_exported_files = "extracted_files"

    # Use glob to find all CSV files ending with "export"
    exported_files = glob.glob(os.path.join(path_exported_files, "*.export.CSV"))

    # Check if there are any exported files
    if not exported_files:
        print("No exported files found.")
    else:
        # Print the number of files before concatenation
        print(f"Number of files before concatenation: {len(exported_files)}")

        # Load each exported file into a DataFrame
        all_exported_data = [pd.read_csv(file) for file in exported_files]

        # Concatenate all DataFrames into one
        unified_exported_data = pd.concat(all_exported_data, ignore_index=True)

        # Add column names if needed (replace colnames with your specific column names)
        colnames = [...]

        # Save the merged and organized data to a new CSV file in the extraction folder
        unified_exported_data.to_csv(os.path.join(extraction_folder, "unified_exported_data.csv"), index=False)
        print("Merged and organized data saved to unified_exported_data.csv in the extraction folder.")

else:
    print(f"Failed to fetch masterfilelist. Status code: {response.status_code}")
