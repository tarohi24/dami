from dami.container import DIContainer

from dami.services.moneyforward import MoneyForwardService
import typer


app = typer.Typer()


@app.command()
def main(path: str = "moneyforward/") -> None:
    container = DIContainer()
    # use default client initiaizer
    service: MoneyForwardService = container.mf_service()
    service.insert_latest_csv()


if __name__ == "__main__":
    app()