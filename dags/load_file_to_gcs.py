
from google.cloud import storage


def load_file_to_gcs(source, destination):
# Replace with your Google Cloud Storage bucket name and file name
  bucket_name = 'wizeline-engine'
  source_file_name = f'tempData/{source}'
  destination_blob_name = f'stage/{destination}'
  
  # Create a client
  client = storage.Client()

  # Get the bucket where you want to upload the file
  bucket = client.bucket(bucket_name)

  # Upload the local CSV file to GCS
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)

  print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}")

  return