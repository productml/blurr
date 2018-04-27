#!/usr/bin/env bash

set -e

if [ ! -d "blurr/core" ]; then
    echo "test.sh should be run from blurr's base directory."
    exit 1
fi

sh scripts/start_dynamodb.sh

# Check if DynamoDB is running.
status="$(ps aux | grep DynamoDBLocal.jar | grep java)"
if [ ! "$status" ]; then
    echo "Local DynamoDB not running"
    exit 1
fi

echo "running tests..."
export PYTHONPATH=`pwd`:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
pipenv run pytest -v --cov=blurr

echo "Done."
