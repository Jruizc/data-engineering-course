terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                  = var.bq_dataset_demo
  friendly_name               = "test"
  description                 = "This is a test description"
  location                    = "EU"
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }

  access {
    role          = "roles/bigquery.dataOwner"
    user_by_email = google_service_account.bqowner.email
  }

  access {
    role   = "READER"
    domain = "hashicorp.com"
  }
}

resource "google_service_account" "bqowner" {
  account_id = "bqowner"
}