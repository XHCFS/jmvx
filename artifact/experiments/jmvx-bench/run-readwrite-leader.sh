#!/bin/bash

# ARGUMENTS: [data size (MB: int)] [buffer size (KB: int)] [delay (msec: int)]

ROLE="LeaderRecorder"
DATASIZE=$1
BUFFERSIZE=$2
DELAYFOR=$3

if [ -z "$JMVX_SOURCED" ]; then
    echo "Environment not set, please run the comment: source env.sh";
    exit 1
fi

#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5555,suspend=n"
#OPS="-DmappedMemoryLocation=/Volumes/RAMDisk/ -DreadsFromDisk=false -DavoidObjectSerialization=true" 
CIRC=""
if [ "$CIRC_BUFFER" != "" ]; then
	CIRC="-DmappedMemorySize=$CIRC_BUFFER" # -DmappedMemoryLocation=/Volumes/RAMDisk/"
	echo "Set circ buffer to $CIRC_BUFFER"
fi

$JAVA_HOME/bin/java -cp $JMVX_BENCH_INSTRUMENTED/target/$JMVX_BENCH_JAR \
-Xbootclasspath/p:$INSTALL_DIR/rt.jar:$JMVX_JAR \
$CIRC \
$DEBUG \
$MEM \
-Dlogfile="$ROLE.log" \
-Djmvx.role=$ROLE \
-DnumTasks=1 \
-DpoolSize=1 \
-DdataSize=$DATASIZE \
-DbufferSize=$BUFFERSIZE \
-DdelayFor=$DELAYFOR \
microbench.MicroBenchmark io.ReadWriteBenchmark

