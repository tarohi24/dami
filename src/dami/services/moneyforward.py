from dataclasses import dataclass, field
import json

from dami.ext.bq import BQPolarsHandler
from dami.ext.gcs import GCSHandler, GCSLocation
from dami.settings import GCP_PROJECT, PROJECT_ROOT
from dami.types.bq import BQField, BQTable


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
            (PROJECT_ROOT / "bigquery/schemas/moneyforward.json").read_text()
        )
        self.bq_table = BQTable(
            project=GCP_PROJECT,
            dataset="finance",
            table="moneyforward",
            fields=[BQField(**col) for col in columns],
        )

    def insert_latest_csv(self):
        last_csv_path = self.gcs_handler.get_latest_blob(self.gcs_dir, suffix=".csv")
        if last_csv_path is None:
            raise FileNotFoundError(
                f"No files found in GCS path: {self.gcs_dir.get_uri()}"
            )
        df = self.gcs_handler.download_df(last_csv_path)
        # delete the files

        self.bq_handler.insert_df(df, self.bq_table)
