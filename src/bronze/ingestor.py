import requests
from datetime import datetime, timezone, timedelta
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # /app/src/bronze

def pullDataFromGithub(url, file_name):
    try:
        response = requests.get(url)
        response.raise_for_status()
        temp_file_loc = os.path.join(BASE_DIR, "tmp", f"{file_name}.gz")
        os.makedirs(os.path.dirname(temp_file_loc), exist_ok=True)
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



def uploadToS3(fetch_time):
    s3_client = boto3.client('s3')
    # file_path = f"bronze/tmp/{fetch_time.strftime('%#H')}.gz"
    hour = fetch_time.strftime("%H")
    file_path = os.path.join(BASE_DIR, "tmp", f"{hour}.gz")
    upload_path = f"archives/{fetch_time.strftime('%Y')}/{fetch_time.strftime('%b')}/{fetch_time.strftime('%d')}/{fetch_time.strftime('%#H')}.gz"
    try:
        s3_client.upload_file(Filename=file_path, Bucket=BUCKET_NAME, Key=upload_path)
        os.remove(file_path)
        print("Local file deleted.")
    except Exception as e:
        print(f"Error during s3 upload at {datetime.now()} for file {file_path}: {e}")
        raise
    print(file_path, BUCKET_NAME, upload_path)


def bronze_main(fetch_time):
    timestamp = fetch_time.strftime("%Y-%m-%d-%-H")
    file_name = fetch_time.strftime("%#H")

    url = f"https://data.gharchive.org/{timestamp}.json.gz"
    pull_status = pullDataFromGithub(url, file_name)
    if pull_status == False:
        print("Error in pulling, exiting now.")
        exit(1)

    uploadToS3(fetch_time)

if __name__ == "__main__":
    import sys
    from datetime import datetime

    fetch_time = datetime.fromisoformat(sys.argv[1])
    bronze_main(fetch_time)

    
