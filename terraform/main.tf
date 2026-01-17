terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.14.1"
    }
  }
  
  backend "gcs" {
    bucket  = "whiro-terraform" # The name of the bucket you created
    prefix  = "demi/"         # The directory path inside the bucket
    # Don't use `file()` here; Terraform GCS backend requires the path as a string
    credentials = "/Users/wataru/.config/gcloud/keys/personal-terraform-admin.json"
  }
}

provider "google" {
  project = local.project_id
  credentials = file("/Users/wataru/.config/gcloud/keys/personal-terraform-admin.json")
}

# create a service account
resource "google_service_account" "dami_sa" {
  account_id   = "${local.namespace}-testing-sa"
  display_name = "Dami Testing Service Account"
}
