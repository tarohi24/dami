from dependency_injector.containers import DeclarativeContainer

from dependency_injector import providers


class DIContainer(DeclarativeContainer):
    # service account
    

    api_client = providers.Singleton(
        ApiClient,
        api_key=config.api_key,
        timeout=config.timeout,
    )

    service = providers.Factory(
        Service,
        api_client=api_client,
    )
