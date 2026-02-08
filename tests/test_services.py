import polars as pl
import pytest

from dami.container import DIContainer
from dami.ext.gcs import GCSLocation
from dami.services.moneyforward import MoneyForwardService
from dependency_injector import providers


class TestMoneyForwardService:
    @pytest.fixture
    def service(self, container: DIContainer) -> MoneyForwardService:
        container.mf_gcs_location.override(
            provider=providers.Object(
                GCSLocation(
                    bucket="whiro-dami-storage",
                    path="mf_records/",
                )
            )
        )
        return container.mf_service()

    @pytest.mark.skip(reason="Requires actual GCS and BQ access")
    def test_insert(self, service: MoneyForwardService):
        service.insert_latest_csv()


def test_pass():
    df = pl.read_csv(
        "/Users/wataru/Downloads/収入・支出詳細_2026-01-01_2026-01-31.csv",
        encoding="shift-jis",
    )
    assert "内容" in df.columns
