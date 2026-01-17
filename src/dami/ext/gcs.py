from collections.abc import Callable
from dataclasses import dataclass
from typing import cast
from google.cloud import storage
from google.cloud.storage import Blob

import polars as pl


@dataclass
class GCSLocation:
    bucket: str
    path: str


GCSPath = str | GCSLocation


EXTENSION_TO_LOADER: dict[str, Callable[[bytes], pl.DataFrame]] = {
    "csv": lambda data: pl.read_csv(data),
    "parquet": lambda data: pl.read_parquet(data),
}

class UnsupportedFileTypeError(Exception):
    pass



@dataclass
class GCSHandler:
    client: storage.Client

    def _path_to_location(self, path: GCSPath) -> GCSLocation:
        if isinstance(path, GCSLocation):
            return path
        assert isinstance(path, str)    
        if not path.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {path}")
        path = path[5:]
        bucket_name, blob_name = path.split("/", 1)
        return GCSLocation(bucket=bucket_name, path=blob_name)

    def _get_blob(self, loc: GCSLocation) -> Blob | None:
        bucket = self.client.get_bucket(loc.bucket)
        blob = bucket.get_blob(loc.path)
        return blob
    
    def get_latest_blob(self, prefix: GCSPath) -> Blob | None:
        """
        in a given prefix, get the latest blob
        """
        loc = self._path_to_location(prefix)
        bucket = self.client.get_bucket(loc.bucket)
        blobs = list(self.client.list_blobs(bucket, prefix=loc.path))
        if len(blobs) == 0:
            return None
        latest_blob = max(blobs, key=lambda b: b.updated)
        return latest_blob

    def download_df(self, path: GCSPath) -> pl.DataFrame:
        loc = self._path_to_location(path)
        extension = loc.path.split(".")[-1]
        try:
            loader = EXTENSION_TO_LOADER[extension]
        except KeyError:
            raise UnsupportedFileTypeError(f"Unsupported file type: {extension}")
        bucket = self.client.get_bucket(loc.bucket)
        blob = bucket.get_blob(loc.path)
        if blob is None:
            raise FileNotFoundError(f"File not found: gs://{loc.bucket}/{loc.path}")
        data = blob.download_as_bytes() 
        data = pl.read_csv(data)
        return data