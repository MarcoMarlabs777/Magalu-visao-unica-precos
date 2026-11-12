.PHONY: clean build
.DEFAULT_GOAL := help

# export PYTHONPATH=usr/local/lib/python3.8/site-packages/sness-common/:$(PWD)

.PHONY: help
help: ## Show help menu
	@awk 'BEGIN {FS = ":.*##"; printf "\n Usage:\n  make ... ? [Example: make help] \n \033[36m\033[0m\n"} /^[$$()% a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-35s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

clean:
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info

flake: clean
	@flake8 --max-line-length=79 --per-file-ignores="__init__.py:F401" $(PYTHONPATH)/sness

test: flake
	@pip install .
	@pytest -vv -rxs -W ignore::DeprecationWarning

coverage: flake
	@pip install .
	@pytest -xs -W ignore::DeprecationWarning

install:
	@pip install -e ".[dev]"

build: clean
	python3 -m build

deploy: build
	@twine upload dist/* --repository-url https://pypi.magazineluiza.com.br/
