package edu.uic.cs.jmvx.runtime.pools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This acts as a shadow scheduler for the follower.
 * The leader will assign lamport clock stamps to runnables upon submission
 * Those stamps create the ordering the leader's and follower's schedulers obey
 * Instead of trying to rectify divergences in the follower's scheduler, it uses this one.
 *
 * Instead of using a queue, we have a backing array (jobList)
 * The stamp of the task represents the index of the job in the array
 * When the follower goes to run a job, this scheduler returns a JMVXJob containing
 * a NOP task (a runnable which does nothing)
 * in beforeExecute, the follower gets the stamp of the task the leader thread ran,
 * and acquires that task from the job list.
 * Finally, it replaces the task inside the JMVXJob, effectively subbing the job entirely
 */
public class MVXSafeExecutor extends JMVXThreadPoolExecutor {

    NotReallyAQueue ref;

    public MVXSafeExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new NotReallyAQueue(), threadFactory);
        //silly hack because these two are codependent
        ref = (NotReallyAQueue) getQueue();
        ref.init(this);
        for(int i = corePoolSize; i > 0; i -= prestartAllCoreThreads());
    }

    public JMVXJob takeJobWithStamp(int stamp){
        return ref.takeJobWithStamp(stamp);
    }

    public static class NotReallyAQueue extends PriorityBlockingQueue<Runnable> {

        /*
         * Use job list to randomly access jobs
         * Jobs are given a stamp when they are submitted
         * That stamp will act as their index into the job list
         *
         * Note this will take up a lot of space because we don't downsize the list
         * We could keep track of the lowest stamp left in the pool and clear everything before that
         * and apply an offset in the index calculation to reduce space.
         *
         * Note: once a job is taken off the jobList to run, it will be substituted with the JOB_DONE placeholder
         * So the actual jobs can be garbage collected
         */
        List<JMVXJob> jobList= new ArrayList<>();

        final Runnable NOP = () -> {};
        /*
         * Sentinels with unique references
         * NO_JOB means the job you are looking for hasn't been added yet
         * JOB_DONE means someone else took it and ran the job
         * You will only encounter a JOB_DONE if an error occurs (try to dequeue the same job twice)
         */
        public JMVXJob NO_JOB;
        public JMVXJob JOB_DONE;

        private JMVXThreadPoolExecutor pool;

        private void init(JMVXThreadPoolExecutor pool){
            NO_JOB = createPlaceholder(pool);
            JOB_DONE = createPlaceholder(pool);
            this.pool = pool;
        }

        /* Task returned must be new!
         * The placeholder returned here is the task a worker WILL run
         * But it needs to be mutable so a strategy can modify the wrapped task,
         * effectively changing the task.
         * This means the memory must be unique to whichever worker thread gets it */
        @Override
        public Runnable poll() {
            return createPlaceholder(pool);
        }

        @Override
        public Runnable take() throws InterruptedException {
            return poll();
        }

        @Override
        public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
            return poll();
        }


        /**
         * Returns the number of jobs left in the queue
         * Note: jobs running and completed are still in the jobList, but are set to a constant reference: `JOB_DONE`
         * The number of tasks remaining is: jobList.size() - number of jobs != JOB_DONE
         * As a result, this is O(n) where n is the NUMBER OF JOBS SUBMITTED OVER THE LIFE OF THIS QUEUE.
         * TODO: Add a field to cache the number of jobs remaining?
         * @return the number of jobs remaining
         */
        @Override
        public int size() {
            synchronized (jobList){
                return jobList.size() - (int)jobList.stream().filter((r) -> r == JOB_DONE).count();
            }
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean add(Runnable runnable) {
            JMVXJob j = (JMVXJob) runnable;
            int index = j.$JMVX$getClock();
            if(index < 0) {
                throw new Error("JMVXJob must have a positive valued clock!");
            } else {
                synchronized (jobList) {
                    int inQueue = jobList.size();
                    if (index == inQueue) { //can add at end
                        jobList.add(j);
                    } else if (index > inQueue) { //need placeholders for jobs yet to come
                        for (int i = inQueue; i >= inQueue; i++) {
                            jobList.add(NO_JOB); //add a placeholder
                        }
                    }
                    //set the index of the current job
                    jobList.set(index, j);
                    jobList.notifyAll(); //notify anyone who is waiting for a job
                }
            }
            return true;
        }

        @Override
        public boolean offer(Runnable runnable) {
            return add(runnable);
        }

        public JMVXJob takeJobWithStamp(int stamp){
            while(true) {
                synchronized (jobList) {
                    if (stamp < jobList.size()) {
                        //returns the element we replaced
                        return jobList.set(stamp, JOB_DONE);
                    } else { //our job wasn't ready yet, wait and try again
                        try { //this may be very slow
                            jobList.wait(150);
                        }catch (InterruptedException _){}
                        continue;
                    }
                }
            }
        }

        private JMVXJob createPlaceholder(JMVXThreadPoolExecutor pool){
            return new JMVXJob(NOP, pool.poolId, pool);
        }
    }
}
