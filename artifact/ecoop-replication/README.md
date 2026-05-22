# Introduction

Record/Replay (RR) allows developers to record an execution and then replay it exactly as it was recorded.
RR enables deterministic replay of non-deterministic behaviors in a different environment than the one used for the recording, which can capture complex bugs in production and find the root cause during development.
Unfortunately, support for RR still introduces a non-negligible amount of performance overhead, which limits its applicability.
Two main sources of such overhead in state-of-the-art RR systems are: multi-threading, and I/O bound workloads.
To ensure high-fidelity when replaying multi-threading execution, recordings either capture the total order of events, which the replayer then enforces, or capture a partial order that requires further processing before replay.
Recording also effectively doubles the I/O performed as the recorder needs to perform the original I/O and then record it to a log.
Such increased I/O severely limits the performance of I/O dominated workloads.

In "Optimizing Record/Replay through Relaxed Total Ordering and Multi-Version eXecution" (accepted to ECOOP 2026), we presented two complimentary techniques to reduce the overhead of RR.
First, we introduce Relaxed Total Order (RTO), an online-computable weakening of total order that preserves the cross-thread constraints needed for replay while avoiding unnecessary serialization.
We design RTO to be compatible with Multi-Version eXecution (MVX), enabling online deterministic replay without pre-processing the recording log or heavyweight coordination.
Our prototype implementation on top of an existing state-of-the-art RR system (JMVX) halves recording and replaying overhead.

Second, we combine RR with Multi-Version eXecution (MVX) to eliminate RR's poor performance on I/O-bound workloads.
Our hybrid design uses a follower variant to absorb the extra I/O needed for logging, and to backfill as much I/O as possible from the same underlying system, keeping the user-facing leader off the critical path.
Our prototype reduces the overhead required to record I/O bound programs without penalizing other more common workloads.

This artifact is a virtual machine (VM) image which includes a replication package for the experiments which generated data for the paper.

# Setup

Download the VM image from Zenodo, [DOI 10.5281/zenodo.19643217](10.5281/zenodo.19643217).
You'll need 100 GBs of space to run and complete data collection.
Checksum of the uncompressed image: 281524590bc1dff1cd728be00380bfbb *RTO-artifact.ova

The image was made with [VirtualBox](https://www.virtualbox.org).
We configured the VM to use 8 CPU cores and 8 GBs of RAM.
Please check the settings of your VM to match.

Import the image into VirtualBox and then start it.
The password for the user account is: `p`.

If you opted to use a different number of cores you'll have to change some tasksets in `java-mvx/artifact/scripts/machine-specific.sh`.
Go to that directory and replace lines 21-26 of `machine-specific.sh` with the output of running `affinity.py`.

Open a terminal session.
Goto the artifact via `cd java-mvx/artifact`.
Before being able to run anything, you'll need to set some environment variables via `source env.sh`.
A common environment variable we use is `$ROOT`, which references the root directory of the artifact; we have preconfigured it for you.
*You'll have to re-execute this line per terminal session.*

JMVX must instrument the programs it will record.
We've already run those scripts for you.
If you wish to rerun them, see the Instrumentation section for instructions.

# VM Contents

Source Code for JMVX in `java-mvx/src`.
Artifact for replicating the experiments in our ECOOP 2026 paper in `java-mvx/artifact`.
Benchmark software for said experiments in `java-mvx/artifact/software`.
ECOOP 2026 paper itself in `java-mvx/paper`.
Data for the paper in `java-mvx/data`.

# Kick the tires
Run RR, MVX, and the Hybrid Recorder for `batik`.
This should take 10-15 minutes to complete.

If you have not already, prepare the environment variables via:
- `cd java-mvx/artifact`
- `source env.sh`

Recorder
- `./run-jmvx.sh -b chopin -c recorder.properties -- batik -t 4`

Expected output looks like:

> Version: batik 1.16 (use -p to print nominal benchmark stats)   
> ===== DaCapo unknown batik starting =====   
> About to transcode 1 SVG file(s)   
>    
> Converting bavaria.svg to /home/vboxuser/java-mvx/artifact/./scratch/bavaria.png ... ... success   
> ===== DaCapo unknown batik PASSED in 5168 msec =====   

Also, you should see two directories `logs` and `rec`.
`logs` stores a copy of the standard output and timing data.
`rec` contains the process recording; this is necessary for the replayer to work.

Replayer
- Run the recorder if you have not already
- `./run-jmvx.sh -b chopin -c replayer.properties -- batik -t 4`

The expected output is the same as before *except that the time reported by the harness may differ*.
JMVX allows the timing mechanism of the harness to diverge in order to collect data; described more in a 2024 [paper](https://doi.org/10.1145/3689769) at OOPSLA.
We rely on that to collect timing data.

MVX
- `./run-jmvx.sh -b chopin -l leader.properties -f follower.properties -- batik -t 4`

The expected output is the same as this is running the same benchmark.
The follower runs in the background to the leader and will finish slightly behind.
As a result, the process may appear to hang, but is actually waiting for the follower to finish.
Normal MVX does not make a `rec` directory.

Hybrid MVX/RR - a recorder that runs as an MVX system
- `./run-jmvx.sh -b chopin -l hybrid-leader.properties -f hybrid-follower.properties -- batik -t 4`

Hybrid MVX/RR will create a `rec` directory, like the recorder.

If not errors were thrown, then you are good to go!
The replication package in the next section automates experiments and calls to `run-jmvx.sh`.

# Experiments
A replication package is located at `java-mvx/artifact/ecoop-replication`.
The package contains subdirectories for the following experiments:
- vanilla -- Computes the time to execute uninstrumented versions of the Dacapo benchmarks.
- record -- Runs TO and RTO versions of the recorder.
- replay-and-vc-strategy -- Runs the RTO and TO versions of the replayer with various vector clock synchronization strategies
- mvx -- Runs TO and RTO Multi-Version eXecution experiment.
- hybrid-recorder -- Runs the hybrid MVX/RR recorder for Dacapo and jdd.

Each directory contains a `go.sh` script which will start the experiment.
`go.sh` *assumes it is being run from within the experiment directory!*
*Remember to source the `env.sh` file in `java-mvx/artifact` first!*
Running all of the scripts takes around 5 hours to complete.
The following command will launch all benchmarks when run from inside the replication package:
`find . -name "go.sh" -execdir {} \;`

*Alternatively*, if you wish to capture data on the duration of all experiments and each `go.sh` script you could use:
`/usr/bin/time -o overall-time.txt find . -name "go.sh" -execdir /usr/bin/time -a -o time-per.txt {} \;`
The file `overall-time.txt` contains data on how long all experiments took to run.
Each experiment directory gets a `time-per.txt` file that contains the duration for that particular experiment.

## Differences between VM results and our machine
A few benchmarks behave interact differently with JMVX in the VM.

- `avrora` and `lusearch` take significantly longer to replay.
    - The replayer has a 10 minute timeout which may trigger.
- `fop` fails to replay due to a divergence when loading fonts.
    - We did not experience this failure when testing outside of the VM.
    - `summary.txt` in `replay-and-vc-strategies/<strategy><rto|to>` logs the number of runs passed. 
    - In addition, `overhead.txt` reports no passing runs as `-0.0`.
- Instrumenting the Chopin version of Dacapo fails in the VM because the process exceeds a memory limit.
    - We've included a pre-instrumented version to make up for this.

# Data analysis
The experiment scripts do not perform data analysis.
Subsections below give comments to analyze the data.
For each subsection, you need to `cd` into the directory of the experiment.
Note: the `vanilla` experiment must be analyzed first as it is contains data that will be used to normalize other experiments.
In all cases, the commands provided leave behind human readable tables in `summary.txt` and `overhead.txt` (or some variant on those names).
The scripts are documented in [Data analysis scripts](#man-analysis) section.

Tables in the paper combine multiple `summary.txt` and/or `overhead.txt` files.
Unfortunately, we do not have a script that glues them together.
Each subsection mentions which table the data appears in.
In addition, the Table Mappings section redraws the tables from the paper, with references to the files the data comes from in the replication package.

## vanilla
- `cd results`
- Summarize the data with: `$ROOT/times.pl | tee summary.txt` 
- To simplify analyzing the rest of the experiments, set an environment variable to the full path of the summary file: `export VSUMMARY=readlink -f summary.txt`
- `cd ..`

The time to complete the test Harness in `summary.txt` corresponds to the runtime data in Table 1.

## record
To see runtime overhead:
- Summarize TO data with: 
    - `pushd  to; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rec*; popd;` 
- Summarize RTO data with: 
    - `pushd rto; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rec*; popd;` 
- Compare them with: `$ROOT/delta.pl to/overhead.txt rto/overhead.txt | tee deltas.txt`

The TO runtime overhead data corresponds to the recorder overhead as seen in Table 1 and Table 2a (TO column). 
The RTO runtime overhead data corresponds to the RTO column in Table 2a.

To measure log sizes execute these commands in the `record` directory:
- `$ROOT/avg-size.pl rto/recordings/* | tee rto-sizes.txt`
- `$ROOT/avg-size.pl  to/recordings/* | tee  to-sizes.txt`

The log size data is presented in Table 4 of the paper.


## replay-and-vc-strategy
- Summarize "adaptive" strategy with:  
    - `pushd adaptive`
    - `pushd  to; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rep*; popd;` 
    - `pushd rto; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rep*; popd;` 
    - `popd`
- Summarize "wait" strategy with:  
    - `pushd wait` 
    - `pushd  to; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rep*; popd;` 
    - `pushd rto; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rep*; popd;` 
    - `popd`
- Summarize "spin" strategy with:  
    - `pushd spin`
    - `pushd  to; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rep*; popd;` 
    - `pushd rto; $ROOT/times-and-summary.sh -v $VSUMMARY jmvx-rep*; popd;` 
    - `popd`

The runtime overhead (as measured by the harness) of all three approaches make up Table 5.

## mvx
- Summarize RTO data with:
    - `pushd rto`
    - Leader:   `$ROOT/times-and-summary.sh -v $VSUMMARY -p leader   jmvx-leader*`
    - Follower: `$ROOT/times-and-summary.sh -v $VSUMMARY -p follower jmvx-follower*`
    - `popd`
- Summarize TO data with: 
    - `pushd to`
    - Leader:   `$ROOT/times-and-summary.sh -v $VSUMMARY -p leader   jmvx-leader*`
    - Follower: `$ROOT/times-and-summary.sh -v $VSUMMARY -p follower jmvx-follower*`
    - `popd`

The harness runtime overhead corresponds to Table 3.

## hybrid-recorder
- Summarize observed overhead for Dacapo: 
    - `pushd dacapo` 
    - `$ROOT/times-and-summary.sh -v $VSUMMARY -p leader jmvx-leader*`
    - `popd`
    - Paper compares the hybrid leader's overhead to the recorder from earlier (`record` experiment).
- Summarize observed overhead for jdd:
    - Vanilla jdd: `$ROOT/times.pl jmvx-single-leader* | tee jdd-summary.txt`
    - Recorder:    `$ROOT/times-and-summary.sh -v jdd-summary.txt -p recorder jmvx-rec*`
    - Hybrid:      `$ROOT/times-and-summary.sh -v jdd-summary.txt -p leader   jmvx-leader*`


The runtime overhead for jdd corresponds to Table 6.
*The artifact does not create the `dd` portion of Table 6 because we had issues running `rr` within the VM*
The source code to automate the experiment is present in `java-mvx/artifact/experiments/rr-experiment.sh`.

The runtime overhead for Dacapo corresponds to Table 7.

# Appendix
## Analysis Script Documentation {#man-analysis}

A few scripts do the bulk of the work (for runtime overheads): `times.pl`, `overheads.pl`, `delta.pl`, and `times-and-overhead.sh`.

- `times.pl` \[files\] 
    - reports average time and standard deviation a benchmark took to complete
- `overheads.pl` \[vanilla-summary\] \[role-summary\] 
    - computes runtime overhead, normalizing to the data in `vanilla-summary`
    - summary files are expected to be outputs from `times.pl`
    - This script *assumes each summary file contains the same benchmarks in the same order*
    - e.g., it normalizes data by row, *but does not check that the benchmark keys match*.
- `times-and-overhead.sh` \[-v vanilla-summary\] \[-p prepend\] \[files\]
    - convenience script to run both `times.pl` and `overheads.pl` and leave behind files with the data
    - -v is required!
    - -p prepends text to the output file names, which is useful for encoding MVX data
    - Creates 2 files `summary.txt`/`prepend-summary.txt` and `overhead.txt`/`prepend-overhead.txt`
- `delta.pl` \[overheads-a\] \[overheads-b\]
    - Computes the difference between the runtime over of overheads-a and overheads-b
- `avg-size.pl` \[directories\]
    - Computes the average size of files in the first level of all directories supplied

## Table Mappings

The section provides a reference to relate the tables in the paper to data files generated by the artifact.
This is for your reference to easily find files to use when comparing your results to the paper.
Each subsection corresponds to one table and contains a list of the column name and the data file used for it.
In all cases, the data for the test harness was used from the file.

The data included in `java-mvx/data` is in the same format as the replication package.

### Table 1

- Vanilla: `vanilla/results/summary.txt` 
- Recorder: `record/to/overhead.txt` 
- Old Recorder: JMVX OOPSLA 2024 data
- Replayer: `replay-and-vc-strategies/spin/to/overhead.txt` 
- Old Replayer: JMVX OOPSLA 2024 data

### Table 2

Recorder
- TO: `record/to/overhead.txt`
- RTO: `record/rto/overhead.txt` 
- Delta: `record/delta.txt` 

Replayer
- TO: `replay-and-vc-strategies/wait/to/overhead.txt` 
- RTO: `replay-and-vc-strategies/wait/rto/overhead.txt` 
- Delta: `delta.pl` on the to and rto overhead files

Replayer contains data on the replay with the best vector clock strategy.
For us, this was the `wait` strategy.

### Table 3

Leader
- TO: `mvx/to/leader-overhead.txt` 
- RTO: `mvx/rto/leader-overhead.txt` 
- Delta: `delta.pl` on to and rto leaders 

Follower:
- TO: `mvx/to/follower-overhead.txt` 
- RTO: `mvx/rto/follower-overhead.txt` 
- Delta: `delta.pl` on to and rto followers

### Table 4
- TO (Mb): `record/rto-sizes.txt` 
- RTO (Mb): `record/to-sizes.txt`
- Monitor Events: Computed from `java-mvx/artifact/scripts/analyzer.sh Summary <rec>`
- Optimized: Same as optimized events (output refers to them as "FAST" events)

### Table 5
Breaking this into small tables for the readme.
All of these tables are put together for the paper.

Spin:
- TO: `replay-and-vc-strategies/spin/to/overhead.txt`
- RTO: `replay-and-vc-strategies/spin/rto/overhead.txt`
- Delta: `delta.pl` on the to and rto overhead files

Wait:
- TO: `replay-and-vc-strategies/wait/to/overhead.txt`
- RTO: `replay-and-vc-strategies/wait/rto/overhead.txt`
- Delta: `delta.pl` on the to and rto overhead files

Adaptive:
- TO: `replay-and-vc-strategies/adaptive/to/overhead.txt`
- RTO: `replay-and-vc-strategies/adaptive/rto/overhead.txt`
- Delta: `delta.pl` on the to and rto overhead files

### Table 6
dd:
- Vanilla Runtime: `hybrid-recorder/dd/vanilla-summary.txt`
- rr Overhead: `hybrid-recorder/dd/overhead.txt`

jdd:
- Recorder Overhead: `hybrid-recorder/jdd/recorder-overhead.txt`
- Hybrid Overhead: `hybrid-recorder/jdd/leader-overhead.txt`
- Vanilla Runtime: `hybrid-recorder/jdd/jdd-summary.txt`

### Table 7
`hybrid-recorder/go.sh` does not make TO data.
By the time we realized our mistake we already had uploaded the VM image to Zenodo.
The command `$ROOT/experiments/mvx-experiment.sh hybrid-leader.properties hybrid-follower.properties; mv results dacapo-to` will produce the missing data.

TO:
- Recorder: `hybrid-recorder/dacapo-to/recorder-overhead.txt`
- Hybrid: `hybrid-recorder/dacapo-to/hybrid.overhead.txt`

RTO:
- Recorder: `hybrid-recorder/dacapo/recorder-overhead.txt`
- Hybrid: `hybrid-recorder/dacapo/leader-overhead.txt`


## Instrumentation
To run JMVX, the target program must be instrumented. 
This can be done with the command:

```
java -Xmx4G -Dlogfile="instrumenter.log" -Djmvx.file="<program name>" \
	-cp $JMVX_JAR:<program class path> \
	edu.uic.cs.jmvx.Main -instrument <program install dir> <instrumentated dir>
```

JMVX needs to know the name of the program being instrumented (via the `-Djmvx.file` property).
This property is used to differentiate a target application from the jdk itself, so we can turn off jdk specific instrumentation, speeding up the process.
The program must also be on the class path when instrumenting.
The latter two arguments are: the directory containing the programs bytecode/jar(s) and a directory to place the instrumented code into.

Examples of this command can be found in many files in `/java-mvx/artifact/scripts`.
Instrumentation scripts there are tailored for the JVM, DaCapo, and the H2 server (referred to as JMVX Bench in the code).
Below is a brief descriptions of the (important) instrumentation scripts:

* `instrument-jvm.sh` - Instruments the JVM pointed to by `$JAVA_HOME`, defaults to the jdk supplied in the VM
* `instrument-jvm-nosync.sh` - Same as above, but does not instrument monitors and synchronized methods. 
* `instrument-jvm-tracer.sh` - same as `instrument-jvm.sh`, but instruments for the purpose of tracing system calls.
* `instrument-dacapo.sh` - Instruments the Bach version of DaCapo.
* `instrument-dacapo-nosync.sh` - Same as above, but turns off synchronization instrumentation.
* `instrument-dacapo-chopin.sh` - Instruments the Chopin version of DaCapo. This takes a while, around 30-40 minutes.
* `instrument-dacapo-chopin-nosync.sh` - Same as above, but turns off synchronization instrumentation.

Note: after instrumenting the Chopin version of DaCapo, you need to recompte the checksums on the jars!
If you don't the benchmarks will error out.
This can be done with the commands:

* `compute_md5_chopin.py $DACAPO_CHOPIN_INSTRUMENTED`
* `compute_md5_chopin.py $DACAPO_CHOPIN_INSTRUMENTED_NOSYNC`

The variables `$DACAPO_CHOPIN_INSTRUMENTED` and `$DACAPO_CHOPIN_INSTRUMENTED_NOSYNC` are defined in `env.sh`, which is sourced on VM start up.
