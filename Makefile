SHELL := /bin/zsh
python_version = 3.9

.PHONY: all test docs

lint:
	@isort tog
	@isort tests
	@black tog
	@black tests

typecheck:
	@echo -e "Running type checker"
	@mypy -p tog

test: ## Run the tests.conf
	@pytest --cov=tog --cov-report html --cov-report term:skip-covered tests/

all: lint typecheck test
