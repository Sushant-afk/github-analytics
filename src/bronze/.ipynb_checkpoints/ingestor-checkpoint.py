import requests
from datetime import datetime, timezone, timedelta
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")

def pullDataFromGithub(url, file_name):
    try:
        response = requests.get(url)
        response.raise_for_status()
        #save locally
        temp_file_loc = f"data/bronze/tmp/{file_name}.gz"
        with open(temp_file_loc, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
        print("file saved locally")
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP Error occured: {http_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error: {timeout_err}")
    except requests.exceptions.ConnectionError as connection_err:
        print(f"Connection error occured: {connection_err}")
    except Exception as e:
        print(f"Exception error occured: {e}")
    return False



def uploadToS3(file_name):
    s3_client = boto3.client('s3')
    file_path = f"data/bronze/tmp/{file_name}.gz"
    dt = datetime.now()
    upload_path = f"archives/{dt.strftime("%Y")}/{dt.strftime("%b")}/{dt.strftime("%d")}/{file_name}.gz"
    try:
        s3_client.upload_file(Filename=file_path, Bucket=BUCKET_NAME, Key=upload_path)
    except Exception as e:
        print(f"Error during s3 upload at {datetime.now()} for file {file_path}: {e}")
    print(file_path, BUCKET_NAME, upload_path)

if __name__ == "__main__":
    fetch_time = datetime.now(timezone.utc) - timedelta(hours=6)
    timestamp = fetch_time.strftime("%Y-%m-%d-%#H")
    file_name = fetch_time.strftime("%H")

    url = f"https://data.gharchive.org/{timestamp}.json.gz"
    # pull_status = pullDataFromGithub(url, file_name)
    # if pull_status == False:
    #     print("Error in pulling, exiting now.")
    #     exit(1)

    # uploadToS3(file_name)
    
