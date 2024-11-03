import os 
from zipfile import ZipFile
import requests 
import awswrangler as wr 

def download_file_from_url(
    url: str, 
    local_filename: str, 
    local_dir: str = "."
) -> None:
    response = requests.get(url)
    print(f"Data read from: {url}")
    local_filepath = os.path.join(local_dir, local_filename)
    try:
        with open(local_filepath, 'wb') as file:
            file.write(response.content)
        print(f"Data saved at: {local_filepath}")
    except Exception as e:
        print(f"An error occurred when downloading the file from url: {e}")

def extract_data_from_archive(
    zip_file: ZipFile, 
    extract_dir: str, 
    extension: str = ".csv"
) -> str:
    archive_data_filepath = [x for x in zip_file.namelist() if os.path.splitext(x)[-1] == ".csv"][0]
    archive_data_filename = os.path.basename(archive_data_filepath)
    print(f"Data found at: {archive_data_filepath}")
    extracted_data_filepath = os.path.join(
        extract_dir,
        archive_data_filename
    )
    if os.path.isfile(extracted_data_filepath):
        print(f"Data already exists at: {extracted_data_filepath}")
    else:
        try:
            with open(extracted_data_filepath, "wb") as f:
                f.write(zip_file.read(archive_data_filepath))
            print(f"Data extracted to: {extracted_data_filepath}")
        except Exception as e:
            print(f"An error occurred when extracting the data from the archive: {e}")
    return extracted_data_filepath

def upload_file_to_s3(local_filepath, bucket_name, s3_filename):
    s3_path = f"s3://{bucket_name}/{s3_filename}"
    try:
        wr.s3.upload(local_filepath, s3_path)
    except Exception as e:
        print(f"An error occurred when uploading the file to S3: {e}")

def main():
    # Download file from url
    url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
    archive_dir = os.path.join(
        ".",
        "data",
        "temp"
    )
    archive_filename = "openpowerlifting-latest.zip"
    # download_file_from_url(url, archive_filename, archive_dir)

    # Extract data from archive
    archive_filepath = os.path.join(archive_dir, archive_filename)
    zip_file = ZipFile(archive_filepath, "r")   
    extract_dir = os.path.join(
        ".",
        "data",
        "raw",
        "openpowerlifting",
        "obt"
    )
    extracted_data_filepath = extract_data_from_archive(zip_file, extract_dir)
    
    # Upload the file to S3
    bucket_name = "tdouglas-data-prod-useast2"
    s3_filename = f"data/raw/openpowerlifting/obt/csv/{os.path.basename(extracted_data_filepath)}"
    upload_file_to_s3(extracted_data_filepath, bucket_name, s3_filename)

if __name__ == "__main__":
    main()
    # python data_ingestion.py