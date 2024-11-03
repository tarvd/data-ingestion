import os 
import datetime
from zipfile import ZipFile

import requests
import pandas as pd
import awswrangler as wr 

def download_data_openpowerlifting_lifter(
    url: str = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip",
    zip_filename: str = os.path.join(
        ".", 
        "data", 
        "temp", 
        "openpowerlifting-latest.zip"
    )    
) -> str:
    try:
        # Get a response from the url
        response = requests.get(url)
        print(f"Response received from: {url}")
    except Exception as e:
        print(f"An error occurred when connecting to the website: {e}")

    try:
        # Save the response as an archive file
        with open(zip_filename, 'wb') as file:
            file.write(response.content)
        download_timestamp = pd.Timestamp.utcnow()
        print(f"Archive saved to: {zip_filename}")    
    except Exception as e:
        print(f"An error occurred when downloading the archive file: {e}")

    try:
        # Extract the data file from the archive file
        with ZipFile(zip_filename, "r") as z:
            csv_filename_in_archive = [x for x in z.namelist() if os.path.splitext(x)[-1] == ".csv"][0]
            print(f"Data found at: {csv_filename_in_archive}")
            csv_basename = os.path.basename(csv_filename_in_archive).replace("openpowerlifting","lifter")
            csv_temp_filename = os.path.join(
                ".",
                "data",
                "temp",
                csv_basename
            )
            with open(csv_temp_filename, "wb") as f:
                f.write(z.read(csv_filename_in_archive))
                print(f"Data extracted to: {csv_temp_filename}")
                return (csv_temp_filename, download_timestamp)
    except Exception as e:
        print(f"An error occurred when extracting the data file from the archive file: {e}")

def parse_data_file_to_csv(
    csv_temp_filename: str,
    download_timestamp: pd.Timestamp = None
):
    try:
        # Add downloaded_at column and save to csv
        df = pd.read_csv(csv_temp_filename, dtype=str)
        df['downloaded_at'] = download_timestamp
        print(f"Column added to the data: downloaded_at")
        csv_basename = os.path.basename(csv_temp_filename)
        csv_filename = os.path.join(
            ".",
            "data",
            "raw",
            "openpowerlifting",
            "lifter",
            csv_basename
        )
        df.to_csv(csv_filename, index=False)
        print(f"Data parsed and saved to: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"An error occurred when parsing the data file: {e}")
        
def upload_file_to_s3(
    local_filename: str, 
    bucket_name: str = "tdouglas-data-prod-useast2"
) -> None:
    try:
        # Upload file to S3
        local_basename = os.path.basename(local_filename)
        s3_path = f"s3://{bucket_name}/data/raw/openpowerlifting/lifter/{local_basename}"
        wr.s3.upload(local_filename, s3_path)
        print(f"File uploaded to: {s3_path}")
    except Exception as e:
        print(f"An error occurred when uploading the file to S3: {e}")

def main():
    (csv_temp_filename, download_timestamp) = download_data_openpowerlifting_lifter()
    csv_filename = parse_data_file_to_csv(csv_temp_filename, download_timestamp)
    upload_file_to_s3(csv_filename)

if __name__ == "__main__":
    main()
    # python data_ingestion.py