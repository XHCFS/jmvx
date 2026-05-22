#!/usr/bin/bash
BUFS=(1 512 8192 16382)
DATS=(12800) 
#(1280 12800 128000)

TIME="/usr/bin/time -p"
TIMEOUT="timeout -k 10s 15m"

N=10

for D in ${DATS[@]}; do
  MB=$(( $D / 128 ))
  RESULTS="dd-${MB}mb"
  mkdir -p $RESULTS
  for BUF in ${BUFS[@]}; do
    #set names for particular sizes
    B="${BUF}b"
    #case "$BUF" in
    #    1) B="unbuffered"
    #       ;;
    #    512) B="dd"
    #       ;;
    #    8192) B="java"
    #       ;;
    #    16382) B="big"
    #       ;;
    #esac
    #NOTE dd-unbuffered.txt is the name of the config file for microbench!
    VANLOG="$RESULTS/real-dd-$B.txt"
    RRLOG="$RESULTS/rr-$B.txt"
    #truncate files
    echo "" > $VANLOG
    echo "" > $RRLOG
    #get data
    for I in `seq $N`; do
      $TASKSET_LEADER $TIME $TIMEOUT           dd if=/dev/zero of=/dev/null bs=$BUF count=$D |& tee -a $VANLOG
      $TASKSET_LEADER $TIME $TIMEOUT rr record dd if=/dev/zero of=/dev/null bs=$BUF count=$D |& tee -a $RRLOG
    done
  done
done
