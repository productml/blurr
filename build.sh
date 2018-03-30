#!/usr/bin/env bash

set -e

echo "checking requirements..."
pipenv  --version > /dev/null


echo "installing dependencies..."
pipenv install --dev --ignore-pipfile


echo "formatting and validating code..."
pipenv run yapf -i -r . --style='{allow_split_before_dict_value : false}'
pipenv run mypy . --ignore-missing-imports --disallow-untyped-defs --disallow-untyped-calls


echo "running tests..."
export PYTHONPATH=`pwd`:$PYTHONPATH
pipenv run pytest -v


echo "building package..."
pipenv run python setup.py sdist
pipenv run python setup.py bdist_wheel


echo "Done."
