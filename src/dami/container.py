from pathlib import Path
from typing import Literal
from dependency_injector.containers import DeclarativeContainer

from dependency_injector import providers
from google.cloud import storage
from google.cloud import bigquery

from pydantic_settings import BaseSettings, SettingsConfigDict

from pydantic import model_validator

from dami.ext.bq import BQPolarsHandler
from dami.ext.gcs import GCSHandler


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


class DIContainer(DeclarativeContainer):
    # clients
    storage_client = providers.Singleton(storage.Client)
    bq_client = providers.Singleton(bigquery.Client)
    # ext
    gcs_handler = providers.ThreadLocalSingleton(
        GCSHandler,
        client=storage_client,
    )
    bq_handler = providers.ThreadLocalSingleton(
        BQPolarsHandler,
        client=bq_client,
    )
