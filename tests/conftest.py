import pytest

from dami.container import AppSettings, DIContainer
from dami.settings import SERVICE_ACCOUNT_PATH
from dependency_injector import providers



@pytest.fixture
def container() -> DIContainer:
    app_settings = AppSettings(
        environemnt="dev",
        service_account_path=SERVICE_ACCOUNT_PATH,
    )
    container = DIContainer()
    container.settings.override(providers.Object(app_settings))
    return container
