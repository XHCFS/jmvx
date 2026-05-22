#!/usr/bin/bash
pushd $ROOT
experiments/dacapo-rec.sh all vanilla 10 results
experiments/chopin-rec.sh all vanilla 10 results
popd

mv $ROOT/results .
