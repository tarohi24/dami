resource "google_artifact_registry_repository" "dami_registry" {
  location      = local.default_region
  repository_id = "${local.namespace}-registry"
  description   = "Dami artifact registry"
  format        = "DOCKER"
}

resource "google_artifact_registry_repository_iam_member" "dami_registry_runner_access" {
  for_each = toset(
    [
      "roles/artifactregistry.writer",
      "roles/artifactregistry.reader",
    ]
  )
  location   = google_artifact_registry_repository.dami_registry.location
  repository = google_artifact_registry_repository.dami_registry.name
  role       = each.key
  member     = "serviceAccount:${google_service_account.runner.email}"
}
