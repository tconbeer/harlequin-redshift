.PHONY: check
check:
	uv run ruff format .
	uv run ruff check . --fix
	uv run mypy
	uv run pytest

.PHONY: lint
lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy

.PHONY: test
test:
	uv run pytest

.PHONY: init
init:
	uv sync

.PHONY: clean
clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache profile.html
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

.PHONY: serve
serve:
	uv run harlequin -P None -a redshift $(REDSHIFT_DSN)

.PHONY: serve-read-only
serve-read-only:
	uv run harlequin -P None --read-only -a redshift $(REDSHIFT_DSN)

.PHONY: profile
profile: profile.html

profile.html: $(wildcard src/**/*.py)
	uv run pyinstrument -r html -o profile.html --from-path harlequin -a redshift $(REDSHIFT_DSN)
