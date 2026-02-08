resource "google_cloud_run_v2_service" "default" {
  name     = "${local.namespace}-backend"
  location = "us-central1"
    ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    containers {
      # Use a placeholder image for the very first apply, 
      # or the last known stable image.
      image = "us-central1-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.dami_registry.repository_id}/${local.docker_image_name}:latest"
      
      # Terraform manages env vars, resources, ports
      resources {
        limits = {
          cpu    = "1000m"
          memory = "2Gi"
        }
      }
    }
  }

  # CRITICAL: Ignore fields that Cloud Build modifies
  lifecycle {
    ignore_changes = [
      # 1. Ignore the image tag so Cloud Build can update it freely
      template[0].containers[0].image,
      # 2. Ignore annotations/labels auto-added by Cloud Build/gcloud
      client,
      client_version,
      template[0].labels["client.knative.dev/user-image"],
      template[0].revision,
    ]
  }
}