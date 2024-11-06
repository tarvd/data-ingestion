import os
import hashlib
import json
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


def extract_data_opl_lifter(zip_file: str, ) -> str:
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
    return data_file


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


def upload_file_to_s3(
    local_filename: str, bucket_name: str = "tdouglas-data-prod-useast2"
) -> None:
    try:
        # Upload file to S3
        local_basename = os.path.basename(local_filename)
        s3_path = (
            f"s3://{bucket_name}/data/raw/openpowerlifting/lifter/{local_basename}"
        )
        print(f"File {local_filename} uploaded to {s3_path}")
    except Exception as e:
        print(f"An error occurred when uploading the file to S3: {e}")


def get_checksum(filename: str) -> str:
    hash_func = hashlib.new("sha256")
    with open(filename, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def compare_checksum_to_log(parquet_file, checksum, log_filename):
    key = os.path.basename(parquet_file)
    with open(log_filename, "r") as f:
        log = json.load(f)
    if key in log:
        if checksum == log[key]:
            print("Checksum exists in log")
            return False 
        else:
            pass
    else:
        pass
    log.update({key: checksum})
    with open(log_filename, "w") as g:
        json.dump(log, g, indent=4)
    print("Checksum added to log")
    return True

def main():
    zip_file = os.path.join(
        os.getcwd(), "data", "temp", "openpowerlifting-latest.zip"
    )
    data_file = os.path.join(
        os.getcwd(), "data", "temp", "lifter-2024-11-02-7378b3ce.csv"
    )
    parquet_file = os.path.join(
        os.getcwd(), "data", "temp", "lifter-2024-11-02-7378b3ce.parquet"
    )
    s3_path = f"s3://tdouglas-data-prod-useast2/data/raw/openpowerlifting/lifter/lifter-2024-11-02-7378b3ce.parquet"

    # download_zip_opl_lifter(zip_file)
    data_file = extract_data_opl_lifter(zip_file)
    add_metadata_columns(data_file, download_timestamp=pd.Timestamp.utcnow())
    convert_csv_to_parquet(data_file, parquet_file)
    wr.s3.upload(parquet_file, s3_path)
    print(f"Data uploaded to: {s3_path}")

    
    # This is supposed to use the checksum to compare the data already uploaded to S3 and abort the processing/upload if so.
    # However this is not working, the checksum changes for the same named file downloaded twice in quick succession.
    # checksum = get_checksum(data_filename)
    # proceed_with_upload = compare_checksum(parquet_file, checksum, log_filename)
    # if proceed_with_upload:
    #     upload_file_to_s3(parquet_file)
    #     upload_file_to_s3(log_filename)
    # else:
    #     print(f"Downloaded data already exists in S3 location")

if __name__ == "__main__":
    main()
