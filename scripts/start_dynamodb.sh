#!/bin/bash

TMP_DIR=./.tmp
DYNAMO_JAR=$TMP_DIR/DynamoDBLocal.jar
DYNAMO_URL=https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip

status=""


check_status() {
    status="$(ps aux | grep DynamoDBLocal.jar | grep java)"
}


resolve_dynamo() {
    mkdir -p $TMP_DIR
    TMP_FILE=$TMP_DIR/dynamo.zip
    if [ -f $DYNAMO_JAR ]; then
       echo "$DYNAMO_JAR found"
    else
       echo "$DYNAMO_JAR not found, downloading from $DYNAMO_URL"
       wget $DYNAMO_URL -O $TMP_FILE
       unzip -o -d $TMP_DIR $TMP_FILE
    fi
}


check_status
if [ "$status" ]; then
    echo "Local DynamoDB running"
else
    resolve_dynamo
    echo "Starting local DynamoDB"
    # 25877 is blurr in T9
    java -jar $DYNAMO_JAR -port 25877 -sharedDb &

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
