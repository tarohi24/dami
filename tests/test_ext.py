import time

from dami.container import DIContainer
from dami.ext.bq import BQPolarsHandler
from dami.ext.gcs import (
    GCSHandler,
    GCSLocation,
    BlobNotFoundError,
    UnsupportedFileTypeError,
)
import pytest

import polars as pl

from dami.settings import GS_BUCKET
from dami.types.bq import BQTable, BQField


class TestGCSHandler:
    @pytest.fixture()
    def handler(self, container) -> GCSHandler:
        return container.gcs_handler()

    def test_upload_and_download(self, handler: GCSHandler):
        df = pl.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
            }
        )
        loc = GCSLocation(
            bucket=GS_BUCKET,
            path="test/test_upload_and_download.csv",
        )
        # upload
        handler.upload_bytes(df.write_csv().encode("utf-8"), loc)
        # download
        fetched_df = handler.download_df(blob=handler.get_blob(loc), str_encoding="utf-8")
        assert df.equals(fetched_df)
        # cleanup
        handler.delete_blob(loc)

    def test_get_blob_not_found(self, handler: GCSHandler):
        loc = GCSLocation(
            bucket=GS_BUCKET,
            path="test/non_existent_blob.csv",
        )
        with pytest.raises(BlobNotFoundError):
            handler.get_blob(loc)

    def test_get_latest_blob_no_blobs(self, handler: GCSHandler):
        loc = GCSLocation(
            bucket=GS_BUCKET,
            path="test/no_blobs_prefix/",
        )
        result = handler.get_latest_blob(loc, suffix=".csv")
        assert result is None

    def test_get_latest_blob(self, handler: GCSHandler):
        prefix = "test/test_get_latest_blob/"
        loc1 = GCSLocation(
            bucket=GS_BUCKET,
            path=f"{prefix}file1.csv",
        )
        loc2 = GCSLocation(
            bucket=GS_BUCKET,
            path=f"{prefix}file2.csv",
        )
        handler.upload_bytes(b"data1", loc1)
        time.sleep(1)
        handler.upload_bytes(b"data2", loc2)

        latest_blob = handler.get_latest_blob(
            GCSLocation(bucket=GS_BUCKET, path=prefix), suffix=".csv"
        )
        assert latest_blob is not None
        assert latest_blob.name == loc2.path

        handler.delete_blob(loc1)
        handler.delete_blob(loc2)

    def test_download_unsupported_file_type(self, handler: GCSHandler):
        loc = GCSLocation(
            bucket=GS_BUCKET,
            path="test/unsupported.txt",
        )
        handler.upload_bytes(b"some data", loc)
        blob = handler.get_blob(loc)
        with pytest.raises(UnsupportedFileTypeError):
            handler.download_df(blob, str_encoding=None)
        handler.delete_blob(loc)

    def test_delete_blob(self, handler: GCSHandler):
        loc = GCSLocation(
            bucket=GS_BUCKET,
            path="test/to_be_deleted.csv",
        )
        handler.upload_bytes(b"some data", loc)
        handler.get_blob(loc)  # should not raise
        handler.delete_blob(loc)
        with pytest.raises(BlobNotFoundError):
            handler.get_blob(loc)


class TestBQPolarsHandler:
    @pytest.fixture()
    def bq_handler(self, container: DIContainer) -> BQPolarsHandler:
        return container.bq_handler()

    @pytest.fixture
    def sample_table(self) -> BQTable:
        return BQTable(
            project="strange-oxide-138404",
            dataset="testing",
            table="for_data_test",
            fields=[
                BQField(name="id", type="INTEGER", mode="NULLABLE"),
                BQField(name="name", type="STRING", mode="NULLABLE"),
                BQField(name="value", type="FLOAT", mode="NULLABLE"),
            ],
        )

    def test_validate_df_valid(
        self, bq_handler: BQPolarsHandler, sample_table: BQTable
    ):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "value": [1.1, 2.2],
            }
        )
        # astype to match BQPolarsHandler's expectations
        df = df.with_columns(
            pl.col("id").cast(pl.Int64),
            pl.col("value").cast(pl.Float64),
        )
        bq_handler.validate_df(df, sample_table)

    def test_validate_df_missing_column(
        self, bq_handler: BQPolarsHandler, sample_table: BQTable
    ):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "value": [1.1, 2.2],
            }
        )
        with pytest.raises(ValueError):
            bq_handler.validate_df(df, sample_table)

    def test_validate_df_incorrect_dtype(
        self, bq_handler: BQPolarsHandler, sample_table: BQTable
    ):
        df = pl.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["a", "b"],
                "value": [1.1, 2.2],
            }
        )
        with pytest.raises(TypeError):
            bq_handler.validate_df(df, sample_table)

    def test_update_query(self, bq_handler: BQPolarsHandler, sample_table: BQTable):
        # tear down
        bq_handler.run_update_query(
            f"DELETE FROM `{sample_table.project}.{sample_table.dataset}.{sample_table.table}` WHERE TRUE",
            params={},
        )
        # (1) insert df
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["original_a", "original_b"],
                "value": [1.1, 2.2],
            }
        )
        df = df.with_columns(
            pl.col("id").cast(pl.Int64),
            pl.col("value").cast(pl.Float64),
        )
        bq_handler.insert_df(df, sample_table)

        # (2) update
        update_query = f"UPDATE `{sample_table.project}.{sample_table.dataset}.{sample_table.table}` SET name = @name WHERE id = @id"
        update_params = {
            "id": 1,
            "name": "updated",
        }
        bq_handler.run_update_query(update_query, update_params)

        # (3) fetch
        fetch_query = f"SELECT * FROM `{sample_table.project}.{sample_table.dataset}.{sample_table.table}` WHERE id = @id"
        fetch_params = {"id": 1}
        fetched_df = bq_handler.fetch_df(
            query=fetch_query,
            table=sample_table,
            fields_to_fetch=["id", "name", "value"],
            params=fetch_params,
        )

        # (4) check if the update properly takes place
        assert fetched_df.height == 1
        assert fetched_df["name"][0] == "updated"
        assert fetched_df["id"][0] == 1
        assert fetched_df["value"][0] == 1.1
