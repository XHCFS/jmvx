#!/bin/bash

#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5555,suspend=n"

$JAVA_HOME/bin/java -Xbootclasspath/p:$INSTALL_DIR/rt.jar:$JMVX_JAR -cp $JMVX_JAR -Dlogfile="transformer.log" $DEBUG edu.uic.cs.jmvx.analyzer.$*
