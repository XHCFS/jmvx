## Java MVX

JavaMVX is a multi-version execution system designed to work for managed languages.
It can record the execution of a java program and replay it, in record-replay mode. <br>
Or, it can simultaneous run two different instances of a program where one acts as the leader and the other, a follower.

[//]: # (TODO: Specify use cases)

---

#### Docker Container Setup:
1. [Download Oracle JDK8](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html) and unpack it
2. Start the docker daemon
3. Navigate to where the repo was cloned, then into the _artifact_ directory\
_i.e java-mvx-repo/artifact_
4. Edit the _scripts/machine-specific.sh_ `ROOT` and `JAVA_HOME` to match your machine's specfic directories for the java-mvx-repo/artifact and the unzipped JDK8\
Addtionally, edit the _env.sh_ script line 3 to <br> `source scripts/docker-specific.sh`<br>
5. Source the machine-specific script\
Run:<br>
`source scripts/machine-specific.sh` <br>
6. Build the docker container\
Run:<br>
`./scripts/build-container.sh` <br>
Note, this script will attach your current terminal to the container's terminal. All the instructions below are meant to be done inside of the container. \
If Docker is throwing errors for having the container previously built, run the cleanup script <br> `./scripts/docker-cleanup.sh` <br>
7. Navigate to the _java-mvx/artifact_ directory 
8. Run: `./scripts/affinity.py export`\
 This script will set taskset environment variables according to your machine's resources
9. Run:
<br> `source env.sh` <br>
10. Run: <br> `./scripts/download.sh` <br>
11. Run: <br> `./scripts/build-jmvx.sh` <br>
12. Run: <br> `./scripts/instrument-dacapo.sh` <br>
and <br> `./scripts/instrument-jvm.sh` <br>
13. To attach multiple terminals to the container, open a fresh terminal and Run: 
<br> `docker exec -it java-mvx-container bash` <br>
14. To run in Leader/Follower mode or Record/Replay, follow the appropriate instructions below.
15. To clean up the container\
Run: <br> `./scripts/docker-cleanup.sh`
---

[//]: # (TODO: Environment details)

#### Initial Setup:
1. [Clone](https://github.com/git-guides/git-clone) this repository.
2. Install JDK 8, if not already installed, and copy the path of the JDK directory. Eg: /usr/lib/jvm/jdk1.8.0_202
3. Within the repository, edit *\<repo\>/artifact/scripts/machine-specific.sh*. Replace *JAVA_HOME* with the copied JDK path. Replace *ROOT* with the path of the *artifact* directory.
4. Open a terminal and navigate to the *artifact* directory within the repository path.
5. Run: <br>
`source env.sh` <br>
This will setup the required environment variables.
4. Run: <br>
`./scripts/download.sh` <br>
This will download the [Dacapo benchmark](https://www.dacapobench.org/) in *\<repo\>/artifact/download*
5. Run: <br>
`./scripts/build-jmvx.sh`
6. Run: <br>
`./scripts/instrument-dacapo.sh` <br>
and <br>
`./scripts/instrument-jvm.sh` <br>
This will "instrument", that is, rewrite some of the bytecode to allow for all the JMVX functionality.

---

#### Running Dacapo as Leader-follower:

In this mode, the program can execute only when the leader, the follower and the coordinator that binds to both, are running. <br>
1. Open 3 terminal windows, navigate to the artifact directory and run: `source env.sh` in each window.

2. In one window, run the coordinator: <br>
`./experiments/run-coordinator.sh` <br>

3. Run the leader and follower, each in their own windows, using the scripts that follow. <br>
One of the two dacapo benchmarks can be executed: ***batik*** or ***avrora***. Pass them as arguments to each script: <br>
`./experiments/dacapo/run-leader.sh <benchmark>` <br>
`./experiments/dacapo/run-follower.sh <benchmark>` <br>
For example: <br>
`./experiments/dacapo/run-leader.sh batik`

The benchmark should now start running. <br>
To see the logs, navigate to *artifact* directory, and run: <br>
`tail -F scratch/stdout.log` <br>
Pressing enter on the coordinator will switch roles between the leader and follower. <br>
When the benchmark finishes, *EOFExcpetion* is expected on the coordinator.

---

#### Running Dacapo in Record-replay mode:

1. Open a terminal window, navigate to *\<repo\>/artifact* and run: <br>
`source env.sh`

2. Similar to leader-follower mode, both, ***batik*** and ***avrora*** benchmarks are supported. First, run the recorder with the script: <br>
`./experiments/dacapo/run-recorder.sh <benchmark>` <br>

This will create recording log files in the *artifact* directory.

3. To replay, simply use the run-replayer.sh script by passing the same benchmark as the argument: <br>
`./experiments/dacapo/run-replayer.sh <benchmark>` <br>

[//]: # (Describe a bit about what each thing is actually doing)

---

#### Debugging the execution:

1. Open an IDE that supports remote JVM debugging, like, IntelliJ IDEA. <br>
2. Import the repository as a project.
3. Create a new debug configuration of the type Remote JVM Debug.
  * The Host will be `localhost`
  * And the port will vary according to the application:
    * `5555` for leader and recorder
    * `5556` for follower and replayer
    * `5557` for the coordinator
4. Run the application as shown above, and attach the debugger to it, by simply clicking the debug button in most IDEs.
