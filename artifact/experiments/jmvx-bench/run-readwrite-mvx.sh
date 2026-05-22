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
LEADER_LOG="$LOGS/leader-$PROPS.txt"
FOLLOWER_LOG="$LOGS/follower-$PROPS.txt"

for I in `seq 1 $N`; do
  #echo "MVX (follower has stdout)"
  $TASKSET_COORD ../run-coordinator.sh > coord.txt &
  $TASKSET_FOLLOWER ./run-readwrite-follower.sh $DATA $BUF $DELAY &>> $FOLLOWER_LOG &
  $TASKSET_LEADER ./run-readwrite-leader.sh $DATA $BUF $DELAY |& tee -a $LEADER_LOG

  #waits for the follower to finish
  wait %2

  #clean up the coordinator, bash script and java proc
  pkill -f coordinator
  pkill -f coordinator
  
  if [[ $( jobs -r | wc -l ) > 0 ]]; then
      echo "ABORTING, LEFT MVX PROCESSES ALIVE"
      jobs
  fi
done
