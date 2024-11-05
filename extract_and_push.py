import os
from google.cloud import storage

# Set the path to your service account key
SERVICE_ACCOUNT_KEY_PATH = r'C:/Users/Personal/Miracle/ETL/project1-439615-7a1d4e03c029.json'

# Verify the service account key path
if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
    raise FileNotFoundError(f"Service account key not found: {SERVICE_ACCOUNT_KEY_PATH}")

# Set the environment variable for authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY_PATH

# Initialize Google Cloud Storage client
client = storage.Client()

# Specify the bucket and file details
BUCKET_NAME = 'company_ceo'
FILES_TO_UPLOAD = {
    'company_ceo_data.csv': 'company_ceo_data.csv',  # Source: local file, Destination: blob name in bucket
    'sample_suppliers_data.json': 'sample_suppliers_data.json'  # Example JSON file
}

try:
    # Get the bucket
    bucket = client.get_bucket(BUCKET_NAME)

    # Upload each file to the bucket
    for source_file, destination_blob in FILES_TO_UPLOAD.items():
        if not os.path.exists(source_file):
            print(f"The source file '{source_file}' does not exist.")
            continue

        # Create a blob object for the file
        blob = bucket.blob(destination_blob)

        # Upload the file with a timeout of 600 seconds
        blob.upload_from_filename(source_file, timeout=600)
        print(f"File {source_file} uploaded to {BUCKET_NAME}/{destination_blob}.")

except Exception as e:
    print(f"An error occurred: {e}")
