import pytest

from dami.container import AppSettings, DIContainer
from dami.settings import PROJECT_ROOT
from dependency_injector import providers

from google.cloud import storage
from google.cloud import bigquery


@pytest.fixture
def container():
    app_settings = AppSettings(
        environemnt="dev",
        service_account_path=(
            PROJECT_ROOT / "terraform/.secrets/runner-service-account-key.json"
        ),
    )
    container = DIContainer()
    container.storage_client.override(
        providers.Object(
            storage.Client.from_service_account_json(app_settings.service_account_path)
        )
    )
    container.bq_client.override(
        providers.Object(
            bigquery.Client.from_service_account_json(app_settings.service_account_path)
        )
    )
    return container
