#!/bin/bash

status=""
check_status() {
    status="$(ps aux | grep DynamoDBLocal.jar | grep java)"
}

check_status
if [ "$status" ]; then
    echo "Local DynamoDB running"
else
    echo "Starting local DynamoDB"
    # 25877 is blurr in T9
    java -Djava.library.path=${DYNAMODBPATH?"Need to set DYNAMODBPATH"} -jar $DYNAMODBPATH/DynamoDBLocal.jar -port 25877 -sharedDb &

    end_time=$(($SECONDS + 30))
    interval=2

    while [ "$SECONDS" -lt "$end_time" ];
    do
        check_status
        if [ "$status" ]; then
            exit 0
        fi
        echo "sleeping for ${interval} seconds..."
        sleep ${interval}
    done

    echo "Could not start DynamoDB"
    exit 1
fi
