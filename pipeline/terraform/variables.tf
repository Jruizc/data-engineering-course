variable "project" {
  description = "GCP Project ID"
  default = "terraform-demo-485317"
}

variable "region" {
  description = "GCP Region"
  default = "us-central1"
}

variable "bq_dataset_demo" {
  description = "My Bigquery Data Set"
  default = "demo_dataset"
}

variable "location" {
  description = "Location for resources"
  default = "US"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Name"
  default = "terraform-demo-485317-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}