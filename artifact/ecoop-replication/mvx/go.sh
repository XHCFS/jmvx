#!/bin/sh

#paper showed wait-vc is usually faster, so command is obselete 
#$ROOT/experiments/mvx-experiment.sh leader.properties follower.properties
#mv results rto

#use faster vc strategy
$ROOT/experiments/mvx-experiment.sh leader-wait-vc.properties follower-wait-vc.properties
mv results rto

$ROOT/experiments/mvx-experiment.sh leader-like-v1.properties follower-like-v1.properties
mv results to
