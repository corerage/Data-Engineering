
variable "project" {
  description = "project id"
  default     = "amazing-jetty-455621-s4"
}

variable "credentials" {
  description = "credentials value"
  default     = "./keys.json"
}
variable "region" {
  description = "project region"
  default     = "us-central1"
}


variable "location" {
  description = "project location"
  default     = "US"
}


variable "bigquery_dataset" {
  description = "The bigquery dataset name."
  type        = string
  default     = "example_dataset"
}

variable "storage_bucket_name" {
  description = "The name of the storage bucket."
  type        = string
  default     = "amazing-jetty-455621-s4-demo-bucket"
}

variable "gcs_storage_class" {
  description = "my storage class"
  type        = string
  default     = "STANDARD"
}