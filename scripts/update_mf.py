from pathlib import Path
from typing import Annotated
from dami.container import AppSettings, DIContainer

from dami.ext.gcs import GCSLocation
from dami.services.moneyforward import MoneyForwardService
import typer

from dami.settings import SERVICE_ACCOUNT_PATH
from dependency_injector import providers


app = typer.Typer()


def init_container() -> DIContainer:
    app_settings = AppSettings(
        environemnt="prod",
        service_account_path=SERVICE_ACCOUNT_PATH,
    )
    default_loc = GCSLocation(
        bucket="whiro-dami-storage",
        path="mf_records/",
    )
    container = DIContainer()
    container.settings.override(providers.Object(app_settings))
    container.mf_gcs_location.override(providers.Object(default_loc))
    return container


@app.command()
def main(csv_path: Annotated[Path, typer.Argument(..., help="Path to the CSV file")]) -> None:
    container = init_container()
    service: MoneyForwardService = container.mf_service()
    # Upload the CSV file to GCS
    service.upload_csv_to_gcs(local_path=csv_path)
    service.insert_latest_csv()


if __name__ == "__main__":
    app()