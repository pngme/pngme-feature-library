#!/bin/bash
set -e
cd $(dirname "${BASH_SOURCE[0]}")/..
source .venv/bin/activate

for FEATUREDIR in lib/*; do
    echo "Checking: $FEATUREDIR"

    cd $FEATUREDIR
    black --check .
    mypy .
    cd -
done
