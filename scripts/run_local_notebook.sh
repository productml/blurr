#!/usr/bin/env bash
#
# Loads the tutorials in a local Jupyter Notebook.


set -e

cd ../examples/tutorial

PWD=$(pwd)

echo "checking requirements..."
pipenv  --version > /dev/null

echo "running jupyter"
pipenv install jupyterlab
pipenv run jupyter lab


echo "Done."
