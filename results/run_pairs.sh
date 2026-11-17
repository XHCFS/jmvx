#!/bin/bash
# usage: run_pairs.sh N  -- alternates Unsafe (baseline jar) and JNI runs of avrora record, appends msec to CSV
N=${1:-20}; P=$HOME/jmvx-project; OUT=$P/bench/avrora_record.csv
cd $P/jmvx/artifact && source env.sh >/dev/null 2>&1
LIB=$P/jmvx/native/libjmvxmon.so; JNIJAR=$P/jmvx/target/JavaMVX-0.01-SNAPSHOT.jar; BASEJAR=$P/baseline/JavaMVX-0.01-SNAPSHOT.jar
echo "run,mode,msec" > $OUT
one() { # mode jar
  rm -rf rec; mkdir rec; export JMVX_JAR=$2
  export JAVA_TOOL_OPTIONS="-Djmvx.monitorlib=$LIB"
  ms=$(./experiments/dacapo/run-recorder.sh avrora 2>&1 | grep -oP 'PASSED in \K[0-9]+')
  echo "$3,$1,${ms:-FAIL}" >> $OUT
}
for i in $(seq $N); do
  if (( i % 2 )); then one unsafe $BASEJAR $i; one jni $JNIJAR $i; else one jni $JNIJAR $i; one unsafe $BASEJAR $i; fi
done
