#!/bin/bash
# usage: run_rr.sh BENCH N -- alternates unsafe/jni; each does record then replay; appends msec to CSV
B=${1:-batik}; N=${2:-20}; P=$HOME/jmvx-project; OUT=$P/bench/${B}_rr.csv
cd $P/jmvx/artifact && source env.sh >/dev/null 2>&1
CJ=$DACAPO_CHOPIN_INSTRUMENTED/dacapo-23.11-chopin.jar
LIB=$P/jmvx/native/libjmvxmon.so; JNIJAR=$P/jmvx/target/JavaMVX-0.01-SNAPSHOT.jar; BASEJAR=$P/baseline/JavaMVX-0.01-SNAPSHOT.jar
echo "run,mode,phase,msec" > $OUT
ms() { grep -oP 'PASSED in \K[0-9]+' ; }
one() { # mode jar run
  export JMVX_JAR=$2; export JAVA_TOOL_OPTIONS="-Djmvx.monitorlib=$LIB"
  rm -rf rec
  r=$(./run-jmvx.sh -B $CJ -c recorder.properties -- Harness $B -t 4 2>&1 | ms)
  p=$(./run-jmvx.sh -B $CJ -c replayer.properties -- Harness $B -t 4 2>&1 | ms)
  echo "$3,$1,record,${r:-FAIL}" >> $OUT; echo "$3,$1,replay,${p:-FAIL}" >> $OUT
}
for i in $(seq $N); do
  if (( i % 2 )); then one unsafe $BASEJAR $i; one jni $JNIJAR $i; else one jni $JNIJAR $i; one unsafe $BASEJAR $i; fi
done
rm -rf rec; echo finished >> $P/bench/${B}_rr.done
