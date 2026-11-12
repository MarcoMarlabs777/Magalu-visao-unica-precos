.PHONY: clean build
.DEFAULT_GOAL := help

export PYTHONPATH=$(PWD)

.PHONY: help
help: ## Show help menu
	@awk 'BEGIN {FS = ":.*##"; printf "\n Usage:\n  make ... ? [Example: make help] \n \033[36m\033[0m\n"} /^[$$()% a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-35s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

clean:
	rm -rf build dist *.egg-info

flake: clean
	@flake8 --max-line-length=79 --per-file-ignores="__init__.py:F401" $(PYTHONPATH)/sness

test: flake
	@pytest -vv -rxs -W ignore::DeprecationWarning

coverage: flake
	@pytest -xs -W ignore::DeprecationWarning

build: clean
	python3 -m build

deploy: build
	@twine upload dist/* --repository-url https://pypi.magazineluiza.com.br/
