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

CONFIG=$1 #"recorder-like-v1.properties"

CHOPIN=("avrora" "batik" "fop" "jython" "jme" "pmd" "sunflow" "xalan")
BACH=("h2" "luindex" "lusearch")

THREADS=9
N=10
RESULTS="results"

mkdir -p $RESULTS

BASE=`basename "$CONFIG" ".properties"`
LOG="logs/$BASE.txt"
ANALYSIS="logs/analysis.txt"
RECORDINGS="logs/recordings"

CALLBACK="-c CoreDumpCallback"

#copy the config into the results for bookkeeping
cp $ROOT/configs/$CONFIG $RESULTS/$CONFIG

for B in ${CHOPIN[@]}; do
    #has a taskset
    $BASH -- $ROOT/run-jmvx.sh -b chopin -c $CONFIG -t -n $N -p -a Summary -- $CALLBACK $B -t $THREADS
    mv $LOG $RESULTS/jmvx-rec-$B.txt
    mv $ANALYSIS $RESULTS/analysis-$B.txt
    BACKUPS="$RESULTS/recordings/$B"
    mkdir -p $BACKUPS
    mv $RECORDINGS/*.tgz $BACKUPS
done

for B in ${BACH[@]}; do
    $BASH -- $ROOT/run-jmvx.sh -b bach -c $CONFIG -t -n $N -p -a Summary -- $CALLBACK $B -t $THREADS
    mv $LOG $RESULTS/jmvx-rec-$B.txt
    mv $ANALYSIS $RESULTS/analysis-$B.txt
    BACKUPS="$RESULTS/recordings/$B"
    mkdir -p $BACKUPS
    mv $RECORDINGS/*.tgz $BACKUPS
done
