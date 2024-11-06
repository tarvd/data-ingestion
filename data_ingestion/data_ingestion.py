import os
import sys
import hashlib
import logging
from zipfile import ZipFile

import requests
import pandas as pd
import awswrangler as wr
import pyarrow as pa


def get_checksum(file: str) -> str:
    hash_func = hashlib.new("sha256")
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def download_zip_opl_lifter(
    url: str,
    zip_file: str 
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

def upload_to_s3_opl_lifter(zip_file: str, s3_path: str) -> None:
    with ZipFile(zip_file, "r") as z:
        # Find the data file in the archive
        data_archive_file = [
            x for x in z.namelist() if os.path.splitext(x)[-1] == ".csv"
        ][0]
        print(f"Data found at: $archive/{data_archive_file}")
        
        # Stream data file to S3 in parquet format
        with z.open(data_archive_file, "r") as csv_in_zip:
            chunk_iter = pd.read_csv(csv_in_zip, dtype=str, chunksize=100000)
            for chunk in chunk_iter:
                wr.s3.to_parquet(
                df=chunk,
                dataset=True,
                path=s3_path,
                mode='append'
            )
    print(f"Data uploaded to: {s3_path}")

def main():
    log_file = "logfile.log"
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
    zip_file = os.path.join(os.getcwd(), "data", "temp", "openpowerlifting-latest.zip")
    logging.info(f"Program started with url={url} and zip_file={zip_file}")
    download_zip_opl_lifter(url, zip_file)
    checksum = get_checksum(zip_file)[:32]
    s3_path = f"s3://tdouglas-data-prod-useast2/data/raw/openpowerlifting/lifter/lifter-{checksum}-test.parquet"
    logging.info(f"s3_path={s3_path}, checksum={checksum}")

    if wr.s3.does_object_exist(s3_path):
    # if 1 == 2:
        print(f"File already exists on S3: {s3_path}")
        logging.info(f"Program ended after finding an S3 object at {s3_path}")
        sys.exit()
    else:
        upload_to_s3_opl_lifter(zip_file, s3_path)
        print(f"Data uploaded to: {s3_path}")
        logging.info(f"S3 object uploaded at {s3_path}")


if __name__ == "__main__":
    main()
