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
blurr validate streaming-bts.yml


echo "validating window template..."
blurr validate window-bts.yml


echo "generating session_data_tutorial_1.log..."
blurr transform \
    --streaming-bts=streaming-bts.yml \
    tutorial1-data.log > session_data_tutorial_1.log


echo "generating session_data_tutorial_2_streaming.log..."
blurr transform \
    --streaming-bts=streaming-bts.yml \
    tutorial2-data.log > session_data_tutorial_2_streaming.log
    

echo "generating session_data_tutorial_2_window.log..."
blurr transform \
    --streaming-bts=streaming-bts.yml \
    --window-bts=window-bts.yml \
    tutorial2-data.log > session_data_tutorial_2_window.log


echo "Done."
