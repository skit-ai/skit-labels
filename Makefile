SHELL := /bin/bash
python_version = 3.10

.PHONY: all test docs

lint:
	@isort skit_labels
	@isort tests
	@black skit_labels
	@black tests

typecheck:
	@echo -e "Running type checker"
	@mypy -p skit_labels

test: ## Run the tests.conf
	@pytest --cov=skit_labels --cov-report html --cov-report term:skip-covered tests/

all: lint typecheck test
