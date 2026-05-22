#!/bin/bash

#BMS=("avrora" "batik" "fop" "h2" "jython" "luindex" "lusearch" "pmd" "sunflow" "xalan")
BMS=("h2" "luindex" "lusearch")
COLS=("regular" "large")
RESULTS_DIR="results"

if [ "$1" != "all" ]; then
    BMS=("$1")
fi

if [ "$2" != "all" ]; then
    COLS=("$2")
fi

N=$3

if [ "$4" != "" ]; then
    RESULTS_DIR=$4
fi

THREADS=9
if [ "$5" != "" ]; then
    THREADS=$5
fi

DACAPO_ARGS="-t $THREADS --preserve" #difference from mvx
TIMEOUT="timeout -k 10 10m"
TIMEOUTLONG="timeout -k 10 20m"
TIME=`which time`
VALIDATE_REPLAY=false
ANALYZE_REPLAY=true 

mkdir $RESULTS_DIR &> /dev/null

SCRIPT_DIR="./experiments/dacapo"

for C in ${COLS[@]}; do

    if [ "$C" = "regular" ]; then
        DACAPO_ARGS="-t $THREADS --preserve" #difference from mvx
    elif [ "$C" = "large" ]; then
        DACAPO_ARGS="-t $THREADS --preserve --size large" #difference from mvx
    else
        echo "$C is an unrecognized option"
        exit
    fi

    for B in ${BMS[@]}; do
        LEADER="$RESULTS_DIR/jmvx-leader-recorder-$C-$B.txt"
        FOLLOWER="$RESULTS_DIR/jmvx-follower-recorder-$C-$B.txt"
        COORD="$RESULTS_DIR/jmvx-coordinator-$C-$B.txt"
        
	echo "" > $LEADER
        echo "" > $FOLLOWER
        echo "" > $COORD

        rm leader.txt
        rm follower.txt
        rm coord.txt
        ln -s $LEADER leader.txt
        ln -s $FOLLOWER follower.txt
        ln -s $COORD coord.txt

        for I in $(seq $N); do
            # clear recording dir for hybrid mode
            rm -rf rec; mkdir rec

            echo "Executing JMVX $B run $I"
	    # Launch leader
	    CMD="$TASKSET_LEADER $TIME $TIMEOUT $SCRIPT_DIR/run-leader-recorder.sh $B $DACAPO_ARGS |& tee -a $LEADER"
	    echo "$CMD" |& tee -a $LEADER
	    screen -S leader -d -m bash -c "$CMD"

	    # Launch follower
	    CMD="$TASKSET_FOLLOWER $TIME $TIMEOUT $SCRIPT_DIR/run-follower-recorder.sh $B $DACAPO_ARGS |& tee -a $FOLLOWER"
	    echo "$CMD" |& tee -a $FOLLOWER
	    screen -S follower -d -m bash -c "$CMD"

	    # Launch coordinator
	    CMD="$TASKSET_COORD $TIMEOUT ./experiments/run-coordinator.sh |& tee -a $COORD"
	    echo "$CMD" |& tee -a $COORD
	    screen -S coord -d -m bash -c "$CMD"

	    # Wait for leader
	    screen -r leader

	    # Wait for follower
	    screen -r follower


            # Kill coordinator
            screen -X -S coord kill
            screen -wipe

            rm -rf scratch

            # Check that replay works
            if [ "$VALIDATE_REPLAY" = "true" ]; then
                REPLAYER="$RESULTS_DIR/jmvx-replayer-from-mvx-$B.txt"
	        CMD="$TASKSET_LEADER $TIMEOUT $SCRIPT_DIR/run-replayer-pools.sh $B $DACAPO_ARGS"
	        echo "$CMD" |& tee -a $REPLAYER
                eval "$CMD" |& tee -a $REPLAYER
            fi
 
            if [ "$ANALYZE_REPLAY" = "true" ]; then
                CMD="$ROOT/scripts/analyzer.sh DacapoRunningAnalyzer rec"
                ANALYSIS="$RESULTS_DIR/running-summary-$B.txt"
                echo "$CMD" |& tee $ANALYSIS
                eval "$CMD" |& tee -a $ANALYSIS
            fi

        done
    done
done
