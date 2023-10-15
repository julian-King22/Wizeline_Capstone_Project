terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project     = "wizeline-engine"
  region      = "us-central1"
  credentials = "credentials.json"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                  = "staging"
  friendly_name               = "staging"
  location                    = "US"
}

resource "google_bigquery_dataset" "prod" {
  dataset_id                  = "prod"
  friendly_name               = "prod"
  location                    = "US"
}

resource "google_bigquery_table" "dim_devices" {
  dataset_id = google_bigquery_dataset.prod.dataset_id
  table_id   = "dim_devices"
  deletion_protection = false
  schema = jsonencode([
  {
    "name": "id_dim_devices",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Device Unique ID"
  },
  {
    "name": "device",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Name of device"
  }
])
}

resource "google_bigquery_table" "dim_os" {
  dataset_id = google_bigquery_dataset.prod.dataset_id
  table_id   = "dim_os"
  deletion_protection = false
  schema = jsonencode([
  {
    "name": "id_dim_os",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "os ID"
  },
  {
    "name": "os",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Operating system of choice"
  }
])
}

resource "google_bigquery_table" "dim_location" {
  dataset_id = google_bigquery_dataset.prod.dataset_id
  table_id   = "dim_location"
  deletion_protection = false
  schema = jsonencode([
  {
    "name": "id_dim_location",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Location ID"
  },
  {
    "name": "location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Name of Location"
  }
])
}


resource "google_bigquery_table" "dim_date" {
  dataset_id = google_bigquery_dataset.prod.dataset_id
  table_id   = "dim_date"
  deletion_protection = false
  schema = jsonencode([
  {
    "name": "id_dim_date",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": "Date ID"
  },
  {
    "name": "log_date",
    "type": "DATE",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "day",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "month",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "year",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  }
])
}

resource "google_bigquery_table" "fact_movie_analytics" {
  dataset_id = google_bigquery_dataset.prod.dataset_id
  table_id   = "fact_movie_analytics"
  deletion_protection = false
  schema = jsonencode([
  {
    "name": "customer_id",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "id_dim_os",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "id_dim_location",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "id_dim_device",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "id_dim_date",
    "type": "DATE",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "amount_spent",
    "type": "FLOAT64",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "review_score",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  },
    {
    "name": "review_count",
    "type": "INT64",
    "mode": "NULLABLE",
    "description": ""
  }
])
}

