from dami.ext.gcs import GCSHandler, GCSLocation
import pytest

import polars as pl

from dami.settings import GS_BUCKET


class TestGCSHandler:
    @pytest.fixture()
    def handler(self, container) -> GCSHandler:
        return container.gcs_handler()
    
    def test_upload_and_download(self, handler: GCSHandler):
        df = pl.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        })
        loc = GCSLocation(
            bucket=GS_BUCKET,
            path="test/test_upload_and_download.csv",
        )
        # upload
        handler.upload_bytes(df.write_csv().encode("utf-8"), loc)
        # download
        fetched_df = handler.download_df(blob=handler.get_blob(loc))
        assert df.equals(fetched_df)
