#!/bin/bash

set -e

if [ ! -d "blurr/core" ]; then
    echo "test.sh should be run from blurr's base directory."
    exit 1
fi

ROOT=$(pwd)

docker run -it --rm -p 8888:8888  -v ${ROOT}:/home/jovyan jupyter/datascience-notebook
