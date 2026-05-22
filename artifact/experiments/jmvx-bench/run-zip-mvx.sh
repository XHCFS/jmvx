#!/usr/bin/bash
#have to change shebang b/c mac hahaha normal: /usr/bin/bash
#8 kb, half mb, 1 mb, 5 mb
N=${1:-1} #default: 1

LOGS=logs

mkdir -p $LOGS

LEADER_LOG="$LOGS/leader-zip.txt"
FOLLOWER_LOG="$LOGS/follower-zip.txt"

for I in `seq 1 $N`; do
  rm -rf rec; mkdir rec
  #echo "MVX (follower has stdout)"
  $TASKSET_COORD ../run-coordinator.sh > coord.txt &
  $TASKSET_FOLLOWER ./run-zip.sh FollowerRecorder $DIR "false" &>> $FOLLOWER_LOG &
  $TASKSET_LEADER ./run-zip.sh LeaderRecorder $DIR "false" |& tee -a $LEADER_LOG

  #waits for the follower to finish
  wait -f %+

  #clean up the coordinator, bash script and java proc
  #kill -9 %-
  pkill -f coordinator
  pkill -f coordinator
  
  if [[ $( jobs -r | wc -l ) > 0 ]]; then
      echo "ABORTING, LEFT MVX PROCESSES ALIVE"
      jobs
      exit
  fi
done
