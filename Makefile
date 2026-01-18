.PHONY: format
format:
	uv run ruff check --fix src tests
	uv run ruff format src tests

.PHONY: type
type:
	uv run ty check src tests
