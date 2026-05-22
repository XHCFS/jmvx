#!/bin/bash

# ARGUMENTS: [role] [dir to zip] 

ROLE=$1
VALIDATEWRITES=$2
DEBUGPORT=$3

if [ -z "$JMVX_SOURCED" ]; then
    echo "Environment not set, please run the comment: source env.sh";
    exit 1
fi

DEBUG=""
if [ "$DEBUGPORT" != "" ]; then
    DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=y"
fi
#OPS="-DmappedMemoryLocation=/Volumes/RAMDisk/ -DreadsFromDisk=false -DavoidObjectSerialization=true" 
CIRC=""
if [ "$CIRC_BUFFER" != "" ]; then
	CIRC="-DmappedMemorySize=$CIRC_BUFFER" #-DmappedMemoryLocation=/Volumes/RAMDisk/
	echo "Set circ buffer to $CIRC_BUFFER"
fi

$JAVA_HOME/bin/java -cp $JMVX_BENCH_INSTRUMENTED/target/$JMVX_BENCH_JAR \
-Xbootclasspath/p:$INSTALL_DIR/rt.jar:$JMVX_JAR \
$CIRC \
$DEBUG \
$MEM \
-Dlogfile="$ROLE.log" \
-Djmvx.role=$ROLE \
-DdivergenceHandler=edu.uic.cs.jmvx.runtime.MicrobenchDivergenceHandler \
-DrecordingDir="rec" \
-DvalidateWrites=$VALIDATEWRITES \
-DnumTasks=1 \
-DpoolSize=1 \
-Dtar=$ROOT/software/src.tar \
microbench.io.XZBenchmarkStandAlone

#microbench.MicroBenchmark io.XZBenchmark

#-Ddirectory=$ROOT/software/java-sources \
#microbench.MicroBenchmark io.BZip2Benchmark


