import os 
import zipfile
import requests 
import awswrangler as wr 

def download_file_from_url(
    url: str, 
    local_filename: str, 
    local_dir: str = "."
) -> None:
    response = requests.get(url)
    print(f"Data read from: {url}")
    local_filepath = os.path.join("local_dir", "local_filename")
    try:
        with open(local_filepath, 'wb') as file:
            file.write(response.content)
        print(f"Data saved at: {local_filepath}")
    except Exception as e:
        print(f"An error occurred when downloading the file from url: {e}")

def extract_data_from_archive(
    zip_file: zipfiletype, 
    extract_dir: str, 
    extension: str = ".csv"
) -> ospathjoinfiletype:
    archive_data_filepath = [x for x in zip_file.namelist() if os.path.splitext(x)[-1] = ".csv"][0]
    archive_data_filename = os.path.basename(archive_data_filepath)
    print(f"Data found at: {archive_data_filepath}")
    extracted_data_filepath = os.path.join(
        extract_dir,
        archive_data_filename
    )
    try:
        with open(extracted_data_filepath, "wb") as f:
            f.write(zip_file.read(archive_data_filepath))
        print(f"Data extracted to: {extracted_data_filepath}")
    except Exception as e:
        print(f"An error occurred when extracting the data from the archive: {e}")
    return extracted_data_filepath

# def upload_file_to_s3(local_filename, bucket_name, s3_filename):
#     s3_path = f"s3://{bucket_name}/{s3_filename}"
#     try:
#         wr.s3.upload(local_filename, s3_path)
#     except Exception as e:
#         print(f"An error occurred when uploading the file to S3: {e}")

def main():
    # Download file from url
    url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
    archive_dir = os.path.join(
        ".",
        "data",
        "temp"
    )
    archive_filename = "openpowerlifting-latest.zip"
    download_file_from_url(url, local_filename)

    # Extract data from archive
    zip_file = zipfile.ZipFile(archive_filename, "r")
    print(type(zip_file))
    extract_dir = os.path.join(
        ".",
        "data",
        "raw",
        "openpowerlifting",
        "obt"
    )
    extracted_data_filepath = extract_data_from_archive(zip_file, extract_dir)
    
    # bucket_name = "tdouglas-data-prod-useast2"
    # s3_filename = ""
    # data_filename = extract_data_from_archive(archive_filename, extract_dir)
    # print(data_filename)

if __name__ == "__main__":
    main()