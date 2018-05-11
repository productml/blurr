#!/bin/bash

set -e

echo "checking requirements..."
pipenv  --version > /dev/null
java -version > /dev/null
wget --version > /dev/null

echo "installing dependencies..."
pipenv install --dev --ignore-pipfile

echo "formatting and validating code..."
pipenv run yapf -i -r .
# TODO: fix mypy errors
# pipenv run mypy blurr/*/*.py --ignore-missing-imports --disallow-untyped-defs --disallow-untyped-calls

sh scripts/test.sh

echo "building package..."
pipenv run python setup.py sdist
pipenv run python setup.py bdist_wheel


if [ "$CIRCLECI" == "true" ];
then
    echo "publishing coverage report..."
    pipenv run coveralls
else
    echo "build not running in circleci, coverage not published."
fi


echo "Done."
