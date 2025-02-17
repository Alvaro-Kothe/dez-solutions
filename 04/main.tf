terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.8"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

data "google_project" "default" {}

resource "random_id" "bucket" {
  prefix      = "dez-"
  byte_length = 8
  keepers     = { bq_db = google_bigquery_dataset.default.dataset_id }
}

resource "google_storage_bucket" "default" {
  name          = random_id.bucket.hex
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = true
}

resource "google_bigquery_dataset" "default" {
  dataset_id = "raw_nyc_tripdata"
  location   = var.location

  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "external_tables" {
  for_each            = toset(["green", "yellow", "fhv"])
  dataset_id          = google_bigquery_dataset.default.dataset_id
  table_id            = "ext_${each.key}"
  deletion_protection = false
  schema              = file("./schemas/${each.key}.json")

  external_data_configuration {
    autodetect    = true
    compression   = "GZIP"
    source_format = "CSV"
    source_uris   = ["${google_storage_bucket.default.url}/${each.key}/*.csv.gz"]

    csv_options {
      skip_leading_rows = 1
      field_delimiter   = ","
      quote             = "\""
    }
  }
}
