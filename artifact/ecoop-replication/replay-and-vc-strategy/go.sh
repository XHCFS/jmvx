#!/bin/sh

RECTO=recorder-total-order.properties
RECRTO=recorder.properties

REPSPIN=replayer-spin.properties
REPWAIT=replayer-wait-vc.properties
REPADPT=replayer.properties

#-p keeps it quiet on failure too
mkdir -p spin
#TO
$ROOT/experiments/replayer-experiment.sh $RECTO $REPSPIN
mv results spin/to
#RTO
$ROOT/experiments/replayer-experiment.sh $RECRTO $REPSPIN
mv results spin/rto

mkdir -p wait
#TO
$ROOT/experiments/replayer-experiment.sh $RECTO $REPWAIT
mv results wait/to

#RTO
$ROOT/experiments/replayer-experiment.sh $RECRTO $REPWAIT
mv results wait/rto


mkdir -p adaptive
#TO
$ROOT/experiments/replayer-experiment.sh $RECTO $REPADPT
mv results adaptive/to

#RTO
$ROOT/experiments/replayer-experiment.sh $RECRTO $REPADPT
mv results adaptive/rto

