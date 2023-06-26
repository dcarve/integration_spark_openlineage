help:  ## Show this helper
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

clean:  ## Remove unnecessary files
	@find . | grep -E "(__pycache__|\.pyc)" | xargs rm -rf
	@rm -rf dist ray.egg-info .coverage .pytest_cache

install:  ## Install poetry and project dependencies
	@pip install -r app/requirements.txt
	@pip install -r dev-requirements.txt

tests: clean  ## Run tests with pytest and generate code coverage with pytest-cov
	@pytest --junitxml=pytest-report.xml -vv --cov=app --cov-report=xml --cov-report=term-missing tests/

format:  ## Apply isort and black code formatter
	@black --config pyproject.toml app
	@isort app
	@black --config pyproject.toml tests
	@isort tests
	@black --config pyproject.toml  pipelines
	@isort pipelines

codecheck:  ## Run code check with black and isort
	@black --config pyproject.toml  app --check
	@isort app --check-only
	@black --config pyproject.toml  tests --check
	@isort tests --check-only
	@black --config pyproject.toml  pipelines --check
	@isort pipelines --check-only

bandit:  ## Run security checks using bandit
	@bandit -c pyproject.toml -r app 

run_app:  ## Run app
	@cd app && uvicorn main:app --host 0.0.0.0 --port 8086 --reload

run_test_pipes:  ## Run test pipelines
	@python examples/run_examples.py

build_html:
	@rm -rf docs/build_html/*
	@sphinx-build -M html "docs/source" "docs/build_html"

build_confluence_dev:
	@sphinx-build -M confluence "docs/source" "docs/build_confluence"

build_confluence_prod:
	@sphinx-build -M confluence "docs/source" "docs/build_confluence"