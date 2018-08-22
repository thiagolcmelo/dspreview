.PHONY: default

default: test

install:
	pipenv install --dev --skip-lock

test:
	PYTHONPATH=./src && python -m unittest discover tests/