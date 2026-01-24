variable "credentials" {
  description = "My Credentials File Path"
  default     = "C:/gcp/dtc-de-course-485116-7fa3e6867775.json"
}

variable "project" {
  description = "Project"
  default     = "dtc-de-course-485116"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}


variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "dtc-de-course-485116-terra-bucket"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}



variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}