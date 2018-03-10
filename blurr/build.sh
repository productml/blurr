#!/usr/bin/env bash

set -e

echo "installing dependencies..."
pipenv install --dev --ignore-pipfile

echo "formatting code..."
pipenv run yapf -i -r *.py */**.py --style='{allow_split_before_dict_value : false}'

echo "building package..."
pipenv run python setup.py sdist
pipenv run python setup.py bdist_wheel