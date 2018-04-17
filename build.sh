#!/usr/bin/env bash

set -e

echo "checking requirements..."
pipenv  --version > /dev/null


echo "installing dependencies..."
pipenv install --dev --ignore-pipfile


echo "formatting and validating code..."
pipenv run yapf -i -r .
# TODO: fix mypy errors
# pipenv run mypy blurr/*/*.py --ignore-missing-imports --disallow-untyped-defs --disallow-untyped-calls


echo "running tests..."
export PYTHONPATH=`pwd`:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
pipenv run pytest -v --cov=blurr


echo "building package..."
pipenv run python setup.py sdist
pipenv run python setup.py bdist_wheel


if [ ! -z "CIRCLECI" ];
# post-build commands to execute on CircleCI only
then
    echo "publishing coverage report..."
    pipenv run coveralls
fi


echo "Done."
