from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


from datetime import datetime
from load_file_to_gcs import load_file_to_gcs
from rename_file import rename_file
from log_reviews import log_reviews
from movie_reviews import movie_reviews

from sql_queries import (dim_location_query,
 dim_os_query, dim_devices_query, dim_date_query, fact_movie_analytics
 )

with DAG(
    dag_id="wizeline-capstone-pipeline",
    start_date=datetime(2023,8,30),
    schedule_interval="@hourly",
    catchup=False
  ) as dag:

  log_reviews_from_gcs_transform = PythonOperator(
    task_id =  "load_log_reviews_from_gcs_transform",
    python_callable=log_reviews
  )

  movie_reviews_from_gcs_transform = PythonOperator(
    task_id =  "load_movie_reviews_from_gcs_transform",
    python_callable=movie_reviews
  )

  rename_movie_reviews = PythonOperator(
    task_id='rename_movie_reviews_file',

    python_callable=rename_file,
    op_args=["./tempData/movie_reviews/","part-*.csv"]
  )

  logs_reviews_to_gcs = PythonOperator(
    task_id='load_log_reviews_to_gcs_stage',
    python_callable=load_file_to_gcs,
    op_args=['log_reviews.csv','log_reviews.csv']
  )

  movie_reviews_to_gcs = PythonOperator(
    task_id='load_movie_reviews_to_gcs_stage',
    python_callable=load_file_to_gcs,
    op_args=['movie_reviews/classified_movie_reviews.csv',\
    'classified_movie_reviews.csv']
  )


  sql_to_gcs = PostgresToGCSOperator(
      task_id="load_sql_to_gcs",
      postgres_conn_id="_postgres_connection",
      sql='SELECT * FROM user_purchase',
      bucket="wizeline-engine",
      filename="stage/user_purchase.csv",
      export_format='CSV',
      gzip=False
  )


  load_logs_reviews_bigquery = GCSToBigQueryOperator(
    task_id='logs_to_bigquery',
    bucket='wizeline-engine',
    source_objects=['stage/log_reviews.csv'],
    destination_project_dataset_table="staging.log_reviews",
    write_disposition='WRITE_TRUNCATE',
  )

  load_user_purchase_bigquery = GCSToBigQueryOperator(
    task_id='user_purchase_to_bigquery',
    bucket='wizeline-engine',
    source_objects=['stage/user_purchase.csv'],
    destination_project_dataset_table="staging.user_purchase",
    write_disposition='WRITE_TRUNCATE',
  )

  load_movies_reviews_bigquery = GCSToBigQueryOperator(
    task_id='movies_to_bigquery',
    bucket='wizeline-engine',
    source_objects=['stage/classified_movie_reviews.csv'],
    destination_project_dataset_table="staging.classified_movie_reviews",
    write_disposition='WRITE_TRUNCATE',
  )

  
  create_dim_devices = BigQueryInsertJobOperator(
    task_id='dim_devices_table',
    configuration={
        "query": {
            "query": dim_devices_query,
            "useLegacySql": False  # Use standard SQL syntax
        },
        "writeDisposition": "WRITE_TRUNCATE"  # Overwrite existing data
    }
  )

  create_dim_date = BigQueryInsertJobOperator(
    task_id='dim_date_table',
    configuration={
        "query": {
            "query": dim_date_query,
            "useLegacySql": False  # Use standard SQL syntax
        },
        "writeDisposition": "WRITE_TRUNCATE"  # Overwrite existing data
    }
  )

  create_dim_location = BigQueryInsertJobOperator(
    task_id='dim_location_table',
    configuration={
        "query": {
            "query": dim_location_query,
            "useLegacySql": False  # Use standard SQL syntax
        },
        "writeDisposition": "WRITE_TRUNCATE"  # Overwrite existing data
    }
  )

  create_dim_os = BigQueryInsertJobOperator(
    task_id='dim_os_table',
    configuration={
        "query": {
            "query": dim_os_query,
            "useLegacySql": False  # Use standard SQL syntax
        },
        "writeDisposition": "WRITE_TRUNCATE"  # Overwrite existing data
    }
  )

  create_fact_analytics = BigQueryInsertJobOperator(
    task_id='fact_movie_table',
    configuration={
        "query": {
            "query": fact_movie_analytics,
            "useLegacySql": False  # Use standard SQL syntax
        },
        "writeDisposition": "WRITE_TRUNCATE"  # Overwrite existing data
    }
  )



  [log_reviews_from_gcs_transform, movie_reviews_from_gcs_transform] >> rename_movie_reviews

  rename_movie_reviews >> [logs_reviews_to_gcs, movie_reviews_to_gcs] >> sql_to_gcs
  
  sql_to_gcs >> [load_logs_reviews_bigquery, load_user_purchase_bigquery] >> \
  load_movies_reviews_bigquery

  load_movies_reviews_bigquery >> [create_dim_devices, create_dim_date, \
  create_dim_location, create_dim_os ] >> \
  create_fact_analytics