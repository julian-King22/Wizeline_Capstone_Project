from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import current_timestamp, col, array_contains, when


def define_spark_session():    
  # Create a Spark session
  spark = SparkSession.builder.appName("Movie Review").getOrCreate()
  return spark

def read_file_from_gcs(spark, bucket_name, file_path):
  # Define the GCS file url
  gcs_path = f"gs://{bucket_name}/{file_path}"
  
  # Read the CSV file from GCS into a DataFrame
  df = spark.read.csv(gcs_path, header=True, inferSchema=True)
  return df

def tokenize_words(df):
  # words token using pyspark.ml.feature.Tokenizer
  tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")

  #tranform data with the tokenizer and stop word remover objects
  tokenized_df = tokenizer.transform(df)

  return tokenized_df


def remove_stop_words(tokenized_df):
  #stop words remover
  remover = StopWordsRemover(inputCol="review_token", \
    outputCol="filtered_words", \
    stopWords=StopWordsRemover.loadDefaultStopWords("english"))

  filtered_df = remover.transform(tokenized_df)

  return filtered_df

# Set your GCS bucket name and file path
bucket_name = "wizeline-engine"
file_path = "raw/movie_reviews.csv"


def check_for_good_words(df):

  #check for array column with good words, insert timestamp, do boolean conversion
  df = df.withColumn("positive_review", \
    array_contains(col("filtered_words"), "good"))
  
  return df

def add_timestamp_check_boolean(df):
  df = df.withColumn("insert_date", current_timestamp())
  df = df.withColumn("positive_review", \
    when(df["positive_review"] == True, 1).otherwise(0))
  
  return df

def rename_columns(df):
  # rename columns
  df = df.select(col("cid").alias("customer_id"), \
    col("positive_review"), \
    col("id_review").alias("review_id"))

  return df


def load_to_csv(df):
  output_file_name = "tempData/movie_reviews"

  # Write the DataFrame to CSV with the specified file name
  df.coalesce(1).write.mode("overwrite").csv(output_file_name, header=True)
  return


def movie_reviews():
  spark_session = define_spark_session()
  df_file_from_gcs = read_file_from_gcs(spark_session, bucket_name, file_path)
  words_token = tokenize_words(df_file_from_gcs)
  stop_words_remove = remove_stop_words(words_token)
  check_good_words = check_for_good_words(stop_words_remove)
  insert_timestamp = add_timestamp_check_boolean(check_good_words)
  rename_cols = rename_columns(insert_timestamp)
  load_2_csv = load_to_csv(rename_cols)
  
  return None