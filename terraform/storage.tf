# create a new storage
resource "google_storage_bucket" "this" {
  name                     = "${local.namespace}-storage"
  location                 = "US"
  public_access_prevention = "enforced"
}

# grant the runner service account read/write the storage
resource "google_storage_bucket_iam_member" "runner_writes_storage" {
  bucket = google_storage_bucket.this.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.runner.email}"
}
