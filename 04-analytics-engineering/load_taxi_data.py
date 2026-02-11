import os
import sys
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
import requests
import gzip

# Change this to your bucket name
BUCKET_NAME = "data-engineering-module-03-jgoodman"

COLOR = "green"

YEAR = 2020

# If you authenticated through the GCP SDK you can comment out these two lines
# CREDENTIALS_FILE = "C:\\gcp\\data-engineering-module-03-f5cc93ddc073.json"
# client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
client = storage.Client(project='data-engineering-module-03')


BASE_URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    """Download and unzip a taxi data CSV.GZ file"""
    filename = f"{COLOR}_tripdata_{YEAR}-{month}.csv"
    url = f"{BASE_URL}/{COLOR}/{filename}.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    try:
        print(f"Downloading: {filename}.gz")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Decompress and write CSV file
        with gzip.GzipFile(fileobj=response.raw) as gz_file:
            with open(file_path, 'wb') as out_file:
                out_file.write(gz_file.read())
        
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
        print(f"✓ Successfully saved: {filename} ({file_size:.2f} MB)")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to download {filename}: {e}")
        return False
    except Exception as e:
        print(f"✗ Error processing {filename}: {e}")
        return False


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    # create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    # print("All files processed and verified.")
    # download_file()
