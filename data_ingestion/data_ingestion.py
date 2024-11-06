import os
import hashlib
import json
import logging
from zipfile import ZipFile

import requests
import pandas as pd
import awswrangler as wr


def download_zip_opl_lifter(
    zip_file: str,
    url: str = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip",
) -> None:
    try:
        # Get a response from the url
        response = requests.get(url)
        print(f"Response received from: {url}")
    except Exception as e:
        print(f"An error occurred when connecting to the website: {e}")

    # Save the response as an archive
    with open(zip_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
    print(f"Archive saved to: {zip_file}")


def extract_data_opl_lifter(zip_file: str, ):
    with ZipFile(zip_file, "r") as z:
        # Find the data file in the archive
        data_archive_file = [
            x for x in z.namelist() if os.path.splitext(x)[-1] == ".csv"
        ][0]
        print(f"Data found at: $archive/{data_archive_file}")
        data_basename = os.path.basename(data_archive_file).replace(
            "openpowerlifting", "lifter"
        )
        data_file = os.path.join(os.getcwd(), "data", "temp", data_basename)

        # Extract the data file
        with z.open(data_archive_file, "r") as csv_in_zip:
            with open(data_file, "wb") as f:
                while True:
                    chunk = csv_in_zip.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
    print(f"Data extracted to: {data_file}")


def add_metadata_columns(data_file: str, download_timestamp: pd.Timestamp = None) -> None:
    # Add metadata columns
    output_file = data_file.replace(".csv", "_modified.csv")
    chunk_iter = pd.read_csv(data_file, dtype=str, chunksize=300000)
    for chunk in chunk_iter:
        chunk["downloaded_at"] = download_timestamp
        chunk["created_date"] = download_timestamp.strftime("%Y%m%d")
        chunk.to_csv(output_file, mode="a", index=False)

    # Replace the old file with the new one
    os.remove(data_file)
    os.rename(output_file, data_file)
    print(f"Modified data in: {data_file}")
    print(f"Column added to the data: downloaded_at")
    print(f"Column added to the data: created_date")


def convert_csv_to_parquet(data_file: str, parquet_file: str) -> None:
    # Convert to parquet format
    parquet_basename = (
        f"{os.path.splitext(os.path.basename(data_file))[0]}.parquet"
    )
    parquet_file = os.path.join(
        os.getcwd(), "data", "temp", parquet_basename
    )
    df = pd.read_csv(data_file, dtype=str)
    df.to_parquet(parquet_file, index=False)
    print(f"Data converted and saved to: {parquet_file}")


def get_checksum(file: str) -> str:
    hash_func = hashlib.new("sha256")
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def main():
    log_file = "logfile.log"
    logging.basicConfig(
                    filename=log_file,
                    level=logging.INFO,
                    format='%(asctime)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S') 
    
    zip_file = os.path.join(
        os.getcwd(), "data", "temp", "openpowerlifting-latest.zip"
    )
    download_zip_opl_lifter(zip_file)
    checksum = get_checksum(zip_file)[:32]    
    data_file = os.path.join(
        os.getcwd(), "data", "temp", f"lifter-{pd.Timestamp.utcnow().strftime("%Y%m%d")}-{checksum}.csv"
    )
    parquet_file = data_file.replace(".csv", ".parquet")
    s3_path = f"s3://tdouglas-data-prod-useast2/data/raw/openpowerlifting/lifter/{os.path.basename(parquet_file)}"
    logging.info(f"{zip_file}, {checksum}")

    if wr.s3.does_object_exist(s3_path):
        print(f"File already exists on S3: {s3_path}")
    else:
        extract_data_opl_lifter(zip_file)
        add_metadata_columns(data_file, download_timestamp=pd.Timestamp.utcnow())
        convert_csv_to_parquet(data_file, parquet_file)
        wr.s3.upload(parquet_file, s3_path)
        print(f"Data uploaded to: {s3_path}")
        

if __name__ == "__main__":
    main()
