#!/bin/bash

if [ -z "$JMVX_SOURCED" ]; then
    echo "Environment not set, please run the comment: source env.sh";
    exit 1
fi

CONFIG1=""
CONFIG2=""
MVXLIKE=0
BENCH=""
DEBUG1=""
DEBUG2=""
DUMP=""
MAIN="" #is empty when -B is used
TIME="/usr/bin/time"
TIMEOUT=""
N=1 #default to 1 iter
PRESERVE=0 #save the recording if any
JMVXBOOTSTRAP="-Xbootclasspath/p:$INSTALL_DIR/rt.jar:$JMVX_JAR"
ANALYSIS="" #analysis to run a recording after execution
#check if the time command is installed
#NOT THE BUILT-IN FROM THE SHELL
which -a time | grep -q $TIME
if [ $? -eq 0 ]; then 
    #add posix compliance flag
    TIME="$TIME -p"
else
    echo "$TIME not found, omitting..."
    TIME=""
fi

#convention to the arguments
#-a (lowercase) relies on some knowledge of jmvx infastructure
#    to provide a shortcut to running common code
#    e.g., -c knows where the configs live and makes
#    the majority of the path for you
#-A (uppercase) uses a path
#    this is designed to be a catch all, 
#    if you want to use a particular file/benchmark
#    that the script normally doesn't support,
#    you can supply that path without rewriting
#    1 or more bash scripts (like the old ways...)

while getopts 'ha:b:B:c:C:d:D:f:F:l:L:n:ptT:u' opt; do
    case "$opt" in
        a) ANALYSIS=$OPTARG
           ;;
        b) case "$OPTARG" in
               bach)
                   #callback for memory statistics
                   BENCH=$DACAPO_INSTRUMENTED
                   MAIN="Harness" # -c CoreDumpCallback"
                   ;;
               chopin)
                   BENCH=$DACAPO_CHOPIN_INSTRUMENTED
                   MAIN="Harness" # -c CoreDumpCallback"
                   ;;
               bench)
                   BENCH=$JMVX_BENCH_INSTRUMENTED/target/jmvxBench-jar-with-dependencies.jar
                   MAIN="microbench.MicroBenchmark"
                   ;;
               *) echo "unknown benchmark $OPTARG"
                  echo "Supported -b options: bach bench chopin"
                  exit
                  ;;
           esac
           ;;
        B) BENCH=$OPTARG
           ;;
        c) CONFIG1=$ROOT/configs/$OPTARG
           ;;
        C) CONFIG1=$OPTARG
           ;;
        f) CONFIG2=$ROOT/configs/$OPTARG
           MVXLIKE=1
           ;;
        F) CONFIG2=$OPTARG
           MVXLIKE=1
           ;;
        l) CONFIG1=$ROOT/configs/$OPTARG
           MVXLIKE=1
           ;;
        L) CONFIG1=$OPTARG
           MVXLIKE=1
           ;;
        n) N=$OPTARG
           ;;
        p) PRESERVE=1
           ;;
        d) echo "Debug mode activated"
           DEBUG1="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$OPTARG,suspend=y"
           DEBUG2="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$(( OPTARG + 1)),suspend=y"
           ;;
        D) echo "Debug mode activated"
           DEBUG1="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$OPTARG,suspend=n"
           DEBUG2="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$(( OPTARG + 1)),suspend=n"
           ;;
        t) TIMEOUT="timeout -k 10s 15m"
           ;;
        T) TIMEOUT="timeout -k 10s $OPTARG"
           ;;
#        m) DUMP="-c CoreDumpCallback"
#           ;;
        u) JMVXBOOTSTRAP="-Xbootclasspath/a:$JMVX_JAR"
           ;;
        h) echo "Usages:
RR : ./run-from-config -c <role properties> [-options] <Program Args>
MVX: ./run-from-config -l <leader properties> -f <follower properties> [-options] <Program Args>
 
Options:
  -a [name] - run analyzer.sh [name] on the recording 
              found after each execution
  -b [name] - use a common test benchmark for jmvx
              name = bach | bench | chopin
  -B [path] - set the classpath for java to the project
              at <path>. The project should already be
              instrumented.
  -c [name] - uses the properties file in $ROOT/configs/<name>
  -C [path] - uses the properties file found at path
  -d [port] - Starts VMs in debug mode
              VMs will wait for you to connect
              In MVX mode, Follower uses <port + 1>
  -D [port] - like -d, but jvms do not suspend startup
  -f [name] - follower config file to search for in
              $ROOT/configs/
  -F [path] - use follower config at <path>
  -h        - This help screen
  -l [name] - leader config file to search for in
              $ROOT/configs/
  -L [path] - use leader config at <path>
  -n [n]    - run <n> iterations
              Default is 1
  -p        - preserve gzip'ed tars of recordings
              in logs/recordings
  -t        - use default timeout (15 min)
  -T        - specify timeout length
              (see gnu timeout for formatting)
  -u        - use unmodified rt.jar
              Useful for running jmvx-bench uninstrumented
  -h        - This help screen

Env Variables that are implicitly used by the script
JMVX_SOURCED      - If set, assumes other JMVX related
                    env flags are available. See env.sh
                    for more.
JAVA_HOME         - home directory of the java version to use
MEM               - If set, is assumed to hold the flags
                    and settings for the JVM's heap size
TASKSET_LEADER    - sets cpus for a single config
                    or leader config
TASKSET_FOLLOWER - sets cpus for follower 
                   (mvx only)
TASKSET_COORD    - sets cpus for coordinator 
                   (mvx only)
USER             - stores the user id
                   Builtin in most (all?) shells"
#depracated for now...
#  -m        - Displays memory statistics at the end
#              Requires uninstrumented CoreDumpCallback
#              This flag is implicitly turned on if
#              CORE_DUMP is a non-empty env variable
#CORE_DUMP         - if non empty, implicitly sets -m flag
           exit
           ;;
    esac
done

shift $(( $OPTIND - 1))

#only works for bach and chopin...
#if [ "$CORE_DUMP" != "" ]; then
#	DUMP="-c CoreDumpCallback"
#fi

function nonnull () {
  SETTING=$1
  NAME=$2
  FLAG=$3
  if [ "$SETTING" = "" ]; then
      echo "Please supply a value for $NAME with $FLAG"
      exit 1
  fi
  return
}

#check that required args are set
if [ $MVXLIKE -eq 1 ]; then
    nonnull "$CONFIG1" "leader config"   "-l or -L"
    nonnull "$CONFIG2" "follower config" "-f or -F"
    echo "CONFIG1 $CONFIG1"
    echo "CONFIG2 $CONFIG2"
else
    nonnull "$CONFIG1" "config"          "-c or -C"
    echo "CONFIG1 $CONFIG1"
fi

nonnull "$BENCH" "benchmark" "-b or -B"

#Hack for one of the devs who uses a mac
#gnu timeout doesn't build for mac systems...
if [ "$TIMEOUT" != "" ]; then
    which timeout &> /dev/null
    if [ $? -eq 1 ]; then
        TIMEOUT=""
        echo "Timeout command not found, omitting timeout..."
    fi
fi

#Don't try to do multiple iterations when debug flag is on
#Debug flag halts vm start until the user connects
#also, jdwp can break if we restart jmvx too fast
#e.g., the debug port is not reclaimed, so the next iteration
#fails to get it
if [ $N -gt 1 ] && [ "$DEBUG1" != "" ]; then
    echo "Cannot run n > 1 iterations when debug flag is on"
    echo "Remove either the iterations (-n) or debug (-d) flag"
    exit
fi

echo "Program Args: $MAIN $*"

#setup some logging 
LOGS=logs

mkdir -p $LOGS

#get the filename of the properties
#use that for the names of the logs files
BASE1=`basename $CONFIG1 .properties`
BASE2=`basename $CONFIG2 .properties`

LOG1="$LOGS/$BASE1.txt"
LOG2="$LOGS/$BASE2.txt"

ANALYSISLOG="$LOGS/analysis.txt"

#get the recording directory used from the config file
#note, we may need a hack for the FollowerRecorder here...
#this would check the LeaderRecorder's config.
#for now, we'll set the recordingDir param in both to avoid
#any issues
REC=$(awk 'BEGIN {FS = "=";} $1 == "recordingDir" { print $2; }' $CONFIG1 )
RECORDINGS="$LOGS/recordings"
mkdir -p $RECORDINGS


for I in `seq -w 1 $N`; do
  if [ $MVXLIKE -eq 1 ]; then
      $TASKSET_COORD $ROOT/experiments/run-coordinator.sh > coord.txt &
      #$TASKSET_FOLLOWER ./run-from-config.sh $FOLLOWER $DEBUGF $DUMP $*  &>> $FOLLOWER_LOG &
      #$TASKSET_LEADER ./run-from-config.sh $LEADER $DEBUGL $DUMP $* |& tee -a $LEADER_LOG

      #start follower/config 2 in background
      $TASKSET_FOLLOWER $TIME $TIMEOUT $JAVA_HOME/bin/java $MEM -Djmvx.properties="$CONFIG2" \
        $JMVXBOOTSTRAP \
        $DEBUG2 -cp $BENCH $MAIN $@ &>> $LOG2 &

      #start leader/config 1 in foreground
      $TASKSET_LEADER $TIME $TIMEOUT $JAVA_HOME/bin/java $MEM -Djmvx.properties="$CONFIG1" \
        $JMVXBOOTSTRAP \
        $DEBUG1 -cp $BENCH $MAIN $@ |& tee -a $LOG1
      #waits for the follower to finish
      wait -f %+

      #clean up the coordinator, bash script and java proc
      #kill -9 %-
      pkill -u $USER -f coordinator
      pkill -u $USER -f coordinator
  else
      $TASKSET_LEADER $TIME $TIMEOUT $JAVA_HOME/bin/java $MEM -Djmvx.properties="$CONFIG1" \
        $JMVXBOOTSTRAP \
        $DEBUG1 -cp $BENCH $MAIN $@ |& tee -a $LOG1
  fi
  if [ -d $REC ]; then
      if [ $PRESERVE -eq 1 ]; then
          ARCHIVE="$RECORDINGS/rec-$I.tgz"
          echo "Recording found, saving gzipped copy to $ARCHIVE"
          $TASKSET_LEADER tar -a -cf $ARCHIVE $REC
      fi

      if [ "$ANALYSIS" != "" ]; then
          echo "Running analysis"
          echo "$ROOT/scripts/analyzer.sh $ANALYSIS $REC" | tee -a $ANALYSISLOG
          $ROOT/scripts/analyzer.sh $ANALYSIS $REC | tee -a $ANALYSISLOG
          echo "" >> $ANALYSISLOG
      fi
  fi

  if [[ $( jobs -r | wc -l ) > 0 ]]; then
      echo "ABORTING, LEFT MVX PROCESSES ALIVE"
      jobs
      exit
  fi
done
