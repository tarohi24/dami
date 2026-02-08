from dataclasses import dataclass, field
import datetime
import json
from pathlib import Path
from typing import cast

import polars as pl
from dami.ext.bq import BQPolarsHandler
from dami.ext.gcs import GCSHandler, GCSLocation
from dami.settings import GCP_PROJECT, PROJECT_ROOT
from dami.types.bq import BQField, BQTable
from loguru import logger


COL_MAPPING: dict[str, str] = {
    "計算対象": "is_calculation_target",
    "日付": "transaction_date",
    "内容": "content",
    "金額（円）": "amount",
    "保有金融機関": "financial_institution",
    "大項目": "major_category",
    "中項目": "minor_category",
    "メモ": "memo",
    "振替": "is_transfer",
    "ID": "transaction_id",
}


@dataclass
class MoneyForwardService:
    bq_handler: BQPolarsHandler
    gcs_handler: GCSHandler
    gcs_dir: GCSLocation
    # I don't inject bq_table by DIContainer because
    # this service depends on the specific table
    bq_table: BQTable = field(init=False)

    def __post_init__(self):
        columns = json.loads(
            (PROJECT_ROOT / "bigquery/schema/moneyforward.json").read_text()
        )
        self.bq_table = BQTable(
            project=GCP_PROJECT,
            dataset="finance",
            table="moneyforward",
            fields=[BQField(**col) for col in columns],
        )

    def upload_csv_to_gcs(self, local_path: Path) -> None:
        filename = local_path.name
        loc = GCSLocation(
            bucket=self.gcs_dir.bucket,
            path=f"{self.gcs_dir.path}/{filename}",
        )
        self.gcs_handler.upload_bytes(
            data=local_path.read_bytes(),
            loc=loc,
        )
        logger.info(f"Uploaded {local_path} to GCS: {loc.get_uri()}")

    def insert_latest_csv(self) -> None:
        last_csv_path = self.gcs_handler.get_latest_blob(self.gcs_dir, suffix=".csv")
        if last_csv_path is None:
            raise FileNotFoundError(
                f"No files found in GCS path: {self.gcs_dir.get_uri()}"
            )
        df = self.gcs_handler.download_df(last_csv_path, str_encoding="shift-jis")
        # update
        df = df.rename(COL_MAPPING).with_columns(
            pl.col("transaction_date").str.to_date()
        )
        # delete old records
        table_name = self.bq_table.get_bq_table_id()
        self.bq_handler.run_update_query(
            f"DELETE FROM {table_name} WHERE transaction_date BETWEEN @start_date AND @end_date",
            params={
                "start_date": cast(datetime.date, df["transaction_date"].min()),
                "end_date": cast(datetime.date, df["transaction_date"].max()),
            },
        )
        # delete the files
        self.bq_handler.insert_df(df, self.bq_table)
        logger.info("Inserted latest CSV data into BigQuery")



