#!/bin/bash

if [ -z "$JMVX_SOURCED" ]; then
    echo "Environment not set, please run the comment: source env.sh";
    exit 1
fi

#hack for one of the devs
#macs use old version of zshell with
#like bash 3.?
#This gets around screwing up the shebangs
#across all the files
BASH=`which bash`


RECORDER=$1
LEADER=$2
FOLLOWER=$3 
DATA=$4

if [ "$DATA" = "" ]; then
  echo "Please provide a size"
  exit 1
fi

RESULTS="dd-${DATA}mb"
mkdir -p $RESULTS

#copy configs for our records
cp $ROOT/configs/$RECORDER $RESULTS/$RECORDER
cp $ROOT/configs/$LEADER $RESULTS/$LEADER
cp $ROOT/configs/$FOLLOWER $RESULTS/$FOLLOWER

N=10

RECORDINGS="logs/recordings"
#not good with relative paths
RECORDERLOG="logs/${RECORDER%.properties}.txt"
LEADERLOG="logs/${LEADER%.properties}.txt"
FOLLOWERLOG="logs/${FOLLOWER%.properties}.txt"
SINGLELOG="logs/single-leader.txt"

I=0

#Buffer sizes
#   1  - unbuffered
# 512  - dd default
#8192  - Java BufferedInputStream default
#16382 - 16kb buffer
BUFS=(1 512 8192 16382)

for BUF in ${BUFS[@]}; do
    #unique name in case things go awry
    CONFIG="dd-config-$I.txt"
    B="${BUF}b" #part of log file name
    I=$(( 1 + $I ))

    #set names for particular sizes
    #case "$BUF" in
    #    1) CONFIG="dd-unbuffered.txt"
    #       B="unbuffered"
    #       ;;
    #    512) CONFIG="dd-default.txt"
    #       B="dd"
    #       ;;
    #    8192) CONFIG="dd-java.txt"
    #       B="java"
    #       ;;
    #    16382) CONFIG="dd-big.txt"
    #       B="big"
    #       ;;
    #esac

    echo "#AUTOGEN'ED
benchmark=io.ReadWriteBenchmark
dataSize=$DATA
bufferSize=$BUF
delayFor=0
numTasks=1
poolSize=1" > $CONFIG
    #microbench uses a ref to jmvx in its source
    #so this is the closest we can get to an uninstrumented run
    echo "BASELINE"
    $BASH -- $ROOT/run-jmvx.sh -b bench -c single-leader.properties -t -n $N -- $CONFIG
    mv $SINGLELOG $RESULTS/jmvx-single-leader-$B.txt

    echo "RECORDER"
    $BASH -- $ROOT/run-jmvx.sh -b bench -c $RECORDER                -t -n $N -- $CONFIG
    mv $RECORDERLOG $RESULTS/jmvx-rec-$B.txt

    echo "HYBIRD"
    $BASH -- $ROOT/run-jmvx.sh -b bench -l $LEADER -f $FOLLOWER     -t -n $N -- $CONFIG
    mv $LEADERLOG $RESULTS/jmvx-leader-$B.txt
    mv $FOLLOWERLOG $RESULTS/jmvx-follower-$B.txt

    mv $CONFIG $RESULTS/$CONFIG
done

