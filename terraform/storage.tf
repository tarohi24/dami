# create a new storage
resource "google_storage_bucket" "this" {
  name     = "${local.namespace}-storage"
  location = "US"
  public_access_prevention = "enforced"
}
