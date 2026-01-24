from collections.abc import Callable
from dataclasses import dataclass
from google.cloud import storage
from google.cloud.storage import Blob

import polars as pl


@dataclass
class GCSLocation:
    bucket: str
    path: str

    def get_uri(self) -> str:
        return f"gs://{self.bucket}/{self.path}"


GCSPath = str | GCSLocation


BYTES_TO_LOADER: dict[str, Callable[[bytes, str | None], pl.DataFrame]] = {
    "parquet": lambda data, _: pl.read_parquet(data),
    "csv": lambda data, encoding: pl.read_csv(data, encoding=encoding or "utf-8"),
}


class UnsupportedFileTypeError(Exception):
    pass


class BlobNotFoundError(Exception):
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

    def get_blob(self, loc: GCSLocation) -> Blob:
        bucket = self.client.get_bucket(loc.bucket)
        blob = bucket.get_blob(loc.path)
        if blob is None:
            raise BlobNotFoundError(f"Blob not found: {loc.get_uri()}")
        return blob

    def get_latest_blob(self, prefix: GCSPath, suffix: str) -> Blob | None:
        """
        in a given prefix, get the latest blob
        """
        loc = self._path_to_location(prefix)
        bucket = self.client.get_bucket(loc.bucket)
        blobs = [
            b
            for b in self.client.list_blobs(bucket, prefix=loc.path)
            if b.name.endswith(suffix)
        ]
        if len(blobs) == 0:
            return None
        latest_blob = max(blobs, key=lambda b: b.updated)
        return latest_blob

    def download_df(self, blob: Blob, str_encoding: str | None) -> pl.DataFrame:
        """
        Specify `str_encoding` when you use STR_TO_LOADER.
        """
        assert blob.name is not None
        extension = blob.name.split(".")[-1]
        data = blob.download_as_bytes()
        # Note that passing encoding to `pl.read_XXX`
        # and passing decoded string are different.
        # the latter may cause issues with some file types.
        try:
            loader = BYTES_TO_LOADER[extension]
        except KeyError:
            raise UnsupportedFileTypeError(
                f"Unsupported file type for BYTES_TO_LOADER: {extension}"
            )
        return loader(data, str_encoding)

    def upload_bytes(self, data: bytes, loc: GCSLocation) -> None:
        bucket = self.client.get_bucket(loc.bucket)
        blob = bucket.blob(loc.path)
        blob.upload_from_string(data)  # you can pass bytes directly

    def delete_blob(self, loc: GCSLocation) -> None:
        bucket = self.client.get_bucket(loc.bucket)
        blob = bucket.blob(loc.path)
        blob.delete()
