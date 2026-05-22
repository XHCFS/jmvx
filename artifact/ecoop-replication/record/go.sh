#!/bin/sh
$ROOT/experiments/recorder-experiment.sh recorder.properties
mv results rto

$ROOT/experiments/recorder-experiment.sh recorder-like-v1.properties
mv results to
