#!/bin/sh

$ROOT/experiments/mvx-experiment.sh hybrid-leader.properties hybrid-follower.properties
mv results dacapo

#NOTE: We need to augment the script to just run 100mb
#      And check the blocksize in the vm to make sure dd copies the right amount of data!
$ROOT/experiments/rr-experiment.sh
mv dd-100mb dd 

$ROOT/experiments/microbench-experiment.sh recorder.properties hybrid-leader.properties hybrid-follower.properties 100
mv dd-100mb jdd 
