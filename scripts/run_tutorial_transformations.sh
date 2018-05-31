#!/usr/bin/env bash

# Use this script to run the tutorial transformations under docs/examples/tutorial
# using blurr source code instead of the version installed from pypi.
#
# Results are generated under the original location: examples/tutorial

set -e

cd ..

TUTORIAL_PATH=docs/examples/tutorial

echo "generating session_data_tutorial_1.log..."
pipenv run python -m blurr transform \
    --streaming-bts=${TUTORIAL_PATH}/tutorial1-streaming-bts.yml \
    ${TUTORIAL_PATH}/tutorial1-data.log > ${TUTORIAL_PATH}/session_data_tutorial_1.log


echo "generating session_data_tutorial_2_streaming.log..."
pipenv run python -m blurr transform \
    --streaming-bts=${TUTORIAL_PATH}/tutorial2-streaming-bts.yml \
    ${TUTORIAL_PATH}/tutorial2-data.log > ${TUTORIAL_PATH}/session_data_tutorial_2_streaming.log


echo "generating session_data_tutorial_2_window.log..."
pipenv run python -m blurr transform \
    --streaming-bts=${TUTORIAL_PATH}/tutorial2-streaming-bts.yml \
    --window-bts=${TUTORIAL_PATH}/tutorial2-window-bts.yml \
    ${TUTORIAL_PATH}/tutorial2-data.log > ${TUTORIAL_PATH}/session_data_tutorial_2_window.log

echo "data generated in ${TUTORIAL_PATH}"
