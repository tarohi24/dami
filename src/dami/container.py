from pathlib import Path
from typing import Literal
from dependency_injector.containers import DeclarativeContainer

from dependency_injector import providers
from google.cloud import storage
from google.cloud import bigquery

from pydantic_settings import BaseSettings, SettingsConfigDict

from pydantic import model_validator

from dami.ext.bq import BQPolarsHandler
from dami.ext.gcs import GCSHandler, GCSLocation
from dami.services.moneyforward import MoneyForwardService


AppEnv = Literal["dev", "prod"]


class AppSettings(BaseSettings):
    environemnt: AppEnv
    service_account_path: str | Path | None = None
    model_config = SettingsConfigDict(env_prefix="APP_")

    @model_validator(mode="after")
    def check_mode_consistency(self):
        if self.environemnt == "prod":
            if self.service_account_path is not None:
                raise ValueError(
                    "Production mode is expected to run on Cloud Run, which uses default service account."
                )
        else:
            if self.service_account_path is None:
                raise ValueError("Development mode requires a service account path.")
        return self
    

def inject_storage_client(settings: AppSettings) -> storage.Client:
    assert settings.service_account_path is not None
    return storage.Client.from_service_account_json(
        str(settings.service_account_path)
    )


def inject_bq_client(settings: AppSettings) -> bigquery.Client:
    assert settings.service_account_path is not None
    return bigquery.Client.from_service_account_json(
        str(settings.service_account_path)
    )
    


class DIContainer(DeclarativeContainer):
    settings = providers.Factory(AppSettings)
    # settings
    mf_gcs_location = providers.Factory(GCSLocation)
    # clients
    storage_client = providers.Singleton(inject_storage_client, settings=settings)
    bq_client = providers.Singleton(inject_bq_client, settings=settings)
    # ext
    gcs_handler = providers.ThreadLocalSingleton(
        GCSHandler,
        client=storage_client,
    )
    bq_handler = providers.ThreadLocalSingleton(
        BQPolarsHandler,
        client=bq_client,
    )
    # services
    mf_service = providers.ThreadLocalSingleton(
        MoneyForwardService,
        bq_handler=bq_handler,
        gcs_handler=gcs_handler,
        gcs_dir=mf_gcs_location,
    )
