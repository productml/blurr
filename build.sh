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
export PYTHONPATH=`pwd`:$PYTHONPATH
pipenv run pytest -v --cov=blurr


echo "validating code from docs..."
EXAMPLES=( "tutorial/streaming-dtc.yml" "tutorial/window-dtc.yml" \
           "offer-ai/offer-ai-streaming-dtc.yml" "offer-ai/offer-ai-window-dtc.yml" \
           "frequently-bought-together/fbt-streaming-dtc.yml" "frequently-bought-together/fbt-window-dtc.yml" )
for DTC in "${EXAMPLES[@]}"
do
	pipenv run python -m blurr validate examples/${DTC}
done


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
