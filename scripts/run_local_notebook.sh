#!/usr/bin/env bash

set -e

ROOT=$(pwd)/../

docker run -it --rm -p 8888:8888  -v ${ROOT}:/home/jovyan jupyter/datascience-notebook
