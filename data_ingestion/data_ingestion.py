import os
import hashlib
from zipfile import ZipFile

import requests
import pandas as pd
import awswrangler as wr


def download_data_opl_lifter(
    zip_filename: str,
    url: str = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip",
) -> str:
    try:
        # Get a response from the url
        response = requests.get(url)
        print(f"Response received from: {url}")
    except Exception as e:
        print(f"An error occurred when connecting to the website: {e}")

    # Save the response as an archive
    with open(zip_filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
    print(f"Archive saved to: {zip_filename}")


def extract_data_opl_lifter(zip_filename: str) -> str:
    with ZipFile(zip_filename, "r") as z:
        # Find the data file in the archive
        data_filename_in_archive = [
            x for x in z.namelist() if os.path.splitext(x)[-1] == ".csv"
        ][0]
        print(f"Data found at: $archive/{data_filename_in_archive}")
        data_basename = os.path.basename(data_filename_in_archive).replace(
            "openpowerlifting", "lifter"
        )
        data_temp_filename = os.path.join(os.getcwd(), "data", "temp", data_basename)

        # Extract the data file
        with z.open(data_filename_in_archive, "r") as csv_in_zip:
            with open(data_temp_filename, "wb") as f:
                while True:
                    chunk = csv_in_zip.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
    print(f"Data extracted to: {data_temp_filename}")
    return data_temp_filename


def add_metadata_columns(filename, download_timestamp: pd.Timestamp = None):
    # Add metadata columns
    output_filename = filename.replace(".csv", "_modified.csv")
    chunk_iter = pd.read_csv(filename, dtype=str, chunksize=300000)
    for chunk in chunk_iter:
        chunk["downloaded_at"] = download_timestamp
        chunk["created_date"] = download_timestamp.strftime("%Y%m%d")
        chunk.to_csv(output_filename, mode="a", index=False)

    # Replace the old file with the new one
    os.remove(filename)
    os.rename(output_filename, filename)
    print(f"Modified data in: {filename}")
    print(f"Column added to the data: downloaded_at")
    print(f"Column added to the data: created_date")


def convert_csv_to_parquet(data_temp_filename: str) -> str:
    # Convert to parquet format
    parquet_basename = (
        f"{os.path.splitext(os.path.basename(data_temp_filename))[0]}.parquet"
    )
    parquet_filename = os.path.join(
        os.getcwd(), "data", "raw", "openpowerlifting", "lifter", parquet_basename
    )
    df = pd.read_csv(data_temp_filename, dtype=str)
    df.to_parquet(parquet_filename, index=False)
    print(f"Data parsed and saved to: {parquet_filename}")
    return parquet_filename


def upload_file_to_s3(
    local_filename: str, bucket_name: str = "tdouglas-data-prod-useast2"
) -> None:
    try:
        # Upload file to S3
        local_basename = os.path.basename(local_filename)
        s3_path = (
            f"s3://{bucket_name}/data/raw/openpowerlifting/lifter/{local_basename}"
        )
        wr.s3.upload(local_filename, s3_path)
        print(f"File uploaded to: {s3_path}")
    except Exception as e:
        print(f"An error occurred when uploading the file to S3: {e}")


def checksum_file(filename: str) -> str:
    hash_func = hashlib.new("sha256")
    with open(filename, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def main():
    zip_filename = os.path.join(
        os.getcwd(), "data", "temp", "openpowerlifting-latest.zip"
    )
    download_data_opl_lifter(zip_filename)
    data_temp_filename = extract_data_opl_lifter(zip_filename)
    download_timestamp = pd.Timestamp.utcnow()
    add_metadata_columns(data_temp_filename, download_timestamp)
    parquet_filename = convert_csv_to_parquet(data_temp_filename)
    # upload_file_to_s3(parquet_filename)


if __name__ == "__main__":
    main()
