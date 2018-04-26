#!/usr/bin/env bash

kill $(ps aux | grep DynamoDBLocal.jar | grep java | awk '{print $2}')