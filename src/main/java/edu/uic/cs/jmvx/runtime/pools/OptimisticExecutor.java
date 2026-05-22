package edu.uic.cs.jmvx.runtime.pools;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.vectorclock.WaitNotifyVectorClock;

import java.util.concurrent.*;

/*
This one needs vector clock support
Vector clock identifies when a thread is stuck and
the pool launches another threads running the next job
as it may unblock us

Note: we don't use a cached thread pool to start all jobs because it led to poor performance
With 1000 completely asynchronous, IO bound tasks, a cached pool over saturates the system and loses performance.
This class is meant to fix the issue of over saturation by only saturating when necessary
 */
public class OptimisticExecutor extends JMVXThreadPoolExecutor {

    private final WaitNotifyVectorClock clockRef;
    private volatile int outOfBandJobs = 0;

    public OptimisticExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        for(int i = corePoolSize; i > 0; i -= prestartAllCoreThreads());
        if(JMVXRuntime.clock instanceof WaitNotifyVectorClock){
            clockRef = (WaitNotifyVectorClock) JMVXRuntime.clock;
            clockRef.setOnSuspendHook(() -> {
                /*
                We are stuck, pull a job off the queue
                and start it up out of band

                Note the runnable we start will contain the logic
                to set the return values for futures (see code in FutureTask)
                 */
                Runnable r = getQueue().poll();
                if(r != null) //have a job to start
                    outOfBandJobs++; //update here to prevent termination checks from passing early
                else //no jobs to start
                    return;

                //mimic the behavior of the executor service hooks by wrapping the runnable
                final JMVXJob job = (JMVXJob) r;
                Runnable wrapped = () -> {
                    Throwable thrown = null;
                    try {
                        beforeExecute(Thread.currentThread(), job);
                        job.run();
                    }catch (Throwable t){
                        thrown = t;
                    }finally {
                        afterExecute(job, thrown);
                        outOfBandJobs--;
                    }

                };

                Thread t = new JMVXThreadFactory.JMVXPoolThread(new Thread(wrapped));
                t.start();
                //going to let the jvm handle the cleanup for now
            });
        }else{
            throw new RuntimeException("Cannot use the OptimisticExecutor without the WaitNotifyVectorClock");
        }
    }

    @Override
    public boolean isTerminating() {
        return outOfBandJobs == 0 && super.isTerminating();
    }

    @Override
    public boolean isTerminated() {
        return outOfBandJobs == 0 && super.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        /*
        awaitTermination first because so we actually wait
        this may cause other threads to spin as a result
        eg the thread pool itself has terminated and we are stuck waiting for out of band jobs.
         */
        return super.awaitTermination(timeout, unit) && outOfBandJobs == 0;
    }
}
