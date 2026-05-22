#!/usr/bin/bash
#have to change shebang b/c mac hahaha normal: /usr/bin/bash
N=${1:-1} #default: 1

LOGS=logs

mkdir -p $LOGS

LEADER_LOG="$LOGS/recorder-zip.txt"

for I in `seq 1 $N`; do
  rm -rf rec; mkdir rec
  $TASKSET_LEADER ./run-zip.sh RecorderWithPools $DIR "true" |& tee -a $LEADER_LOG
  
  if [[ $( jobs -r | wc -l ) > 0 ]]; then
      echo "ABORTING, LEFT MVX PROCESSES ALIVE"
      jobs
      exit
  fi
done
