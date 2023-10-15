import pandas as pd
import xml.etree.ElementTree as ET
import gcsfs


def load_data_from_csv(project_id, credentials, bucket_name, file_path):
  
  # Create a GCS file system client
  gcs = gcsfs.GCSFileSystem(project=project_id, token=credentials)
  
  # Read the file from GCS into a Pandas DataFrame
  with gcs.open(f'{bucket_name}/{file_path}', 'rb') as file:
      df = pd.read_csv(file)
  
  return df


def log_reviews_transformation(df):
  # Function to extract data from XML
  def extract_data(xml_string):
      root = ET.fromstring(xml_string)
      log_date = root.find(".//logDate").text
      device = root.find(".//device").text
      os = root.find(".//os").text
      location = root.find(".//location").text
      ip_address = root.find(".//ipAddress").text
      phone_number = root.find(".//phoneNumber").text
      return log_date, device, os, location, ip_address, phone_number

  # Apply the extract_data function to 'log' column and create new columns
  df['log_date'], df['device'], df['os'], df['location'], df['ip'], df['phone_number'] = zip(*df['log'].apply(extract_data))

  # rename column name
  df = df.rename(columns = {'id_review':'log_id'})
  # Drop the 'log' column
  df.drop(columns=['log'], inplace=True)

  return df


def extract_year_month(df):
  df['log_date'] = pd.to_datetime(df['log_date'])

  # # Extract the year, month, and day into separate columns
  df['year'] = df['log_date'].dt.year
  df['month'] = df['log_date'].dt.month_name()
  df['day'] = df['log_date'].dt.day

  return df


def load_to_csv(df):
  # Save the DataFrame to a CSV file
  df.to_csv('tempData/log_reviews.csv', index=False, mode='w')
  return


project_id = 'wizeline-engine'

# Google Cloud Storage credentials file path (JSON key file)
credentials = '/home/kingsolomonifeanyi/wizeline-engine-auth.json'

# GCS bucket and file path
bucket_name = 'wizeline-engine'
file_path = 'raw/log_reviews.csv'


def log_reviews():
  load_log_reviews_df = load_data_from_csv(project_id, credentials, bucket_name, file_path)
  transformed_log_reviews = log_reviews_transformation(load_log_reviews_df)
  log_reviews_clean = extract_year_month(transformed_log_reviews)

  load_2_csv = load_to_csv(log_reviews_clean)
  
  return load_2_csv