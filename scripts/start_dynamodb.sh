#!/usr/bin/env bash

status="$(ps aux | grep DynamoDBLocal.jar | grep java)"

if [ "$status" ]; then
    echo "Local DynamoDB running"
else
    echo "Starting local DynamoDB"
    # 25877 is blurr in T9
    java -Djava.library.path=${DYNAMODBPATH?"Need to set DYNAMODBPATH"} -jar $DYNAMODBPATH/DynamoDBLocal.jar -port 25877 -sharedDb &
    sleep 2
fi