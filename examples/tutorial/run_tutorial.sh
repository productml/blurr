#!/usr/bin/env bash
#
# Use this script to run all the tutorial transformations using Blurr CLI


set -e

PWD=$(pwd)

echo "checking requirements..."
pip  --version > /dev/null

echo "installing blurr..."
pip install blurr

echo "validating streaming template..."
blurr validate streaming-dtc.yml


echo "validating window template..."
blurr validate window-dtc.yml


echo "generating session_data_tutorial_1.log..."
blurr transform \
    --streaming-dtc=streaming-dtc.yml \
    tutorial1-data.log > session_data_tutorial_1.log


echo "generating session_data_tutorial_2_streaming.log..."
blurr transform \
    --streaming-dtc=streaming-dtc.yml \
    tutorial2-data.log > session_data_tutorial_2_streaming.log
    

echo "generating session_data_tutorial_2_window.log..."
blurr transform \
    --streaming-dtc=streaming-dtc.yml \
    --window-dtc=window-dtc.yml \
    tutorial2-data.log > session_data_tutorial_2_window.log


echo "Done."
