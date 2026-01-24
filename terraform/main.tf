terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.14.1"
    }
  }

  backend "gcs" {
    bucket = "whiro-terraform" # The name of the bucket you created
    prefix = "demi/"           # The directory path inside the bucket
    # Don't use `file()` here; Terraform GCS backend requires the path as a string
    credentials = "/Users/wataru/.config/gcloud/keys/personal-terraform-admin.json"
  }
}

provider "google" {
  project     = local.project_id
  credentials = file("/Users/wataru/.config/gcloud/keys/personal-terraform-admin.json")
}

# create a service account
resource "google_service_account" "runner" {
  account_id   = "${local.namespace}-runner-sa"
  display_name = "Dami Runner Service Account"
}

resource "google_service_account_key" "runner" {
  service_account_id = google_service_account.runner.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

resource "local_file" "runner_service_account_key" {
  filename = "${path.module}/.secrets/runner-service-account-key.json"
  content  = base64decode(google_service_account_key.runner.private_key)
}

resource "google_project_iam_member" "runner_roles" {
  for_each = toset(
    [
      "roles/viewer",
      "roles/bigquery.dataEditor",
      "roles/bigquery.jobUser",
      "roles/storage.objectAdmin",
    ]
  )
  project = local.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.runner.email}"
}