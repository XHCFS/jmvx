#!/usr/bin/bash
#8 kb, half mb, 1 mb, 5 mb
DATA=$1
BUF=$2
DELAY=$3
N=1

LOGS=logs

mkdir -p $LOGS
rm -rf rec; mkdir rec

PROPS="${DELAY}ms-${DATA}mb-${BUF}kb"
RECORDER_LOG="$LOGS/recorder-$PROPS.txt"

for I in `seq 1 $N`; do
  $TASKSET_LEADER ./run-readwrite.sh RecorderWithPools $DATA $BUF $DELAY |& tee -a $RECORDER_LOG
done
