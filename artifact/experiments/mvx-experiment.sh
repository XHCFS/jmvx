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

LEADER=$1 
FOLLOWER=$2 

CHOPIN=("avrora" "batik" "fop" "jython" "jme" "pmd" "sunflow" "xalan")
BACH=("h2" "luindex" "lusearch")

THREADS=9
N=10
RESULTS="results"
PRESERVE="--preserve" #change to empty string to disable

mkdir -p $RESULTS

RECORDINGS="logs/recordings"
#not good with relative paths
LEADERLOG="logs/${LEADER%.properties}.txt"
FOLLOWERLOG="logs/${FOLLOWER%.properties}.txt"

#copy configs for our records
cp $ROOT/configs/$LEADER $RESULTS/$LEADER
cp $ROOT/configs/$FOLLOWER $RESULTS/$FOLLOWER

for B in ${CHOPIN[@]}; do
    #has a taskset
    $BASH -- $ROOT/run-jmvx.sh -b chopin -l $LEADER -f $FOLLOWER -t -n $N -p -- $B -t $THREADS $PRESERVE
    mv $LEADERLOG $RESULTS/jmvx-leader-$B.txt
    mv $FOLLOWERLOG $RESULTS/jmvx-follower-$B.txt
    BACKUPS="$RESULTS/recordings/$B"
    mkdir -p $BACKUPS
    mv $RECORDINGS/*.tgz $BACKUPS
done

for B in ${BACH[@]}; do
    $BASH -- $ROOT/run-jmvx.sh -b bach -l $LEADER -f $FOLLOWER -t -n $N -p -- $B -t $THREADS $PRESERVE
    mv $LEADERLOG $RESULTS/jmvx-leader-$B.txt
    mv $FOLLOWERLOG $RESULTS/jmvx-follower-$B.txt
    BACKUPS="$RESULTS/recordings/$B"
    mkdir -p $BACKUPS
    mv $RECORDINGS/*.tgz $BACKUPS
done
