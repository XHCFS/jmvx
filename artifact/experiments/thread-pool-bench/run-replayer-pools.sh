#!/bin/bash

if [ -z "$JMVX_SOURCED" ]; then
    echo "Environment not set, please run the comment: source env.sh";
    exit 1
fi

#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5556,suspend=y"

INPUT_ROLE="-Djmvx.role=ReplayerWithPools"

$JAVA_HOME/bin/java -DrecordingDir="rec" -Dlogfile="follower.log" $MEM -Xbootclasspath/p:$INSTALL_DIR/rt.jar:$JMVX_JAR $DEBUG $INPUT_ROLE -DdivergenceHandler="edu.uic.cs.jmvx.runtime.MicrobenchDivergenceHandler" -cp $JMVX_BENCH_INSTRUMENTED/target/jmvxBench.jar microbench.MicroBenchmark $*
