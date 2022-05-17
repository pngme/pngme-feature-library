#!/bin/bash
set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

python3 -m venv .venv
source .venv/bin/activate

pip install black mypy

for FEATUREDIR in lib/*; do
    echo "\Installing dependencies: $FEATUREDIR"
    pip install -r $FEATUREDIR/requirements.txt
done
