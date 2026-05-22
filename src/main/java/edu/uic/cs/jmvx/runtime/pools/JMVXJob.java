package edu.uic.cs.jmvx.runtime.pools;

import org.apache.log4j.Logger;

import javax.management.RuntimeErrorException;

/**
 * A Runnable with unique id
 * The id is used to track which job this is between leader/follower or recorder/replayer
 * The id should be set on creation/submission of the job
 * And referenced again before starting (to redirect the stream)
 *
 * This class wraps a runnable given to the executor service
 * Good way to test how the system works before moving it all into bytecode
 *
 * Comparable is used by the PriorityBlockingQueue JMVXThreadPoolExecutor uses
 *
 * So object is wrapped by a RunnableFuture inside of the executor service (b/c I call super)
 */
public class JMVXJob implements Runnable, JMVXOrderedEvent {

    static Logger log = Logger.getLogger(JMVXJob.class);

    /* job IS NOT FINAL ON PURPOSE
     * We can override the scheduled job by changing this field in beforeExecute
     * The follower relies on this to ensure thread/job scheduling is consistent with the leader
     */
    private Runnable job;
    private final int poolId; //the id of the pool the job is in
    private final JMVXThreadPoolExecutor pool; //ref to the pool the task came from
    private volatile int id = -1; //set by creating thread, read by worker thread

    public JMVXJob(Runnable job, int poolId, JMVXThreadPoolExecutor pool) {
        this.job = job;
        this.poolId = poolId;
        this.pool = pool;
    }

    @Override
    public void $JMVX$setClock(int newClock) {
        this.id = newClock;
    }

    @Override
    public int $JMVX$getClock() {
        return this.id;
    }

    public int getPoolId() {
        return poolId;
    }

    public JMVXThreadPoolExecutor getPool() {
        return pool;
    }

    public Runnable getJob() {
        return job;
    }

    public void setJob(Runnable job) {
        this.job = job;
    }

    //IDEA is complaining because I am using instrumented bytecode to autocomplete
    //but this method won't exist during compilation
    public void $JMVX$run() {}

    @Override
    public void run() {
        if(id == -1) {
            /*This breaks the regular recorder when using thread pools!
            I need a better way to separate the code base.*/
            log.error("No lamport timestamp set for job scheduling. Missing a spot in Thread Pool task creation.");
            throw new RuntimeErrorException(
                    new Error("Attempting to run a task without a clock value, missing a spot!"));
        }
        job.run();
    }
}