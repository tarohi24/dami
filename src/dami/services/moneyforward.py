from dataclasses import dataclass

from dami.db.common import BQPolarsHandler
from dami.ext.gcs import GCSHandler


@dataclass
class MoneyForwardService:
    bq_handler: BQPolarsHandler
    gcs_handler: GCSHandler

    def insert(self):
        # download csv
        
        # load csv to dataframe
        # insert to BQ
        ...