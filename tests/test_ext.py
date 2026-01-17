from dami.ext.gcs import GCSHandler
import pytest


class TestGCSHandler:
    @pytest.fixture()
    def handler(self, container) -> GCSHandler:
        return container.gcs_handler()
    

    def test_pass(self, handler: GCSHandler):
        pass
