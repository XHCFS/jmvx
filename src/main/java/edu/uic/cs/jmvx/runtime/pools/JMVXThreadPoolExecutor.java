package edu.uic.cs.jmvx.runtime.pools;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Alter to just adding code where we need to in bytecode?
 */
public class JMVXThreadPoolExecutor extends ThreadPoolExecutor {

    static Logger log = Logger.getLogger(JMVXThreadPoolExecutor.class);
    /*Reference to the queue in the superclass
      We have to cast to JMVXOrderedQueue every time we want to access it
      So we store the ref in the constructor to save on time later.
    * */
    //private JMVXOrderedQueue queueRef = null;
    //clock is used to schedule tasks
    private AtomicInteger clock = new AtomicInteger(0);
    public boolean saturate = false;
    /*
    pollId is used to identify this pool and jobs belonging to it
    NOTE: this code makes an assumption that there is no data race between the creation of pools.
    The assumption is based on idea that a program is more likely to shutdown and make a new pool (later),
    rather than have 2 pools running at the same time.

    For instance, the thread pool benchmark makes a pool as part of each experiment,
    but does not run 2 experiments at the same time.
     */
    protected int poolId = -1;
    private static AtomicInteger nextPoolId = new AtomicInteger(0);

    private void init() {
        poolId = nextPoolId.getAndIncrement();
        //queueRef = (JMVXOrderedQueue) getQueue();
    }

    public JMVXThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        //new JMVXOrderedQueue<>(corePoolSize, corePoolSize));
        init();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        //our code before
        JMVXJob job = (JMVXJob) r;
        JMVXRuntime.beforeExecute(t, job);
        super.beforeExecute(t, job);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        if(t instanceof UnsupportedOperationException){
            log.warn("Assumption may be broken:", t);
        }
        JMVXJob job = (JMVXJob) r;
        super.afterExecute(job, t);
        //our code after
        JMVXRuntime.afterExecute(job, t);
        //update scheduling info
        //if(queueRef != null)
        //    queueRef.finishTask();
    }



    /*
    NOTE: CompletionService redefines submit methods
    Hence why I missed them for PMD
    Handling submit is redundant for standard implementations
    They call submit -> execute
    Implementing may be useful for a failsafe
    However, we need to be wary if we call both for the same task
    As the JMVX callbacks are invoked by each method
    (So the recorder may log jobs 1 and 2 for what was really 1 job)
     */
    /*
    @Override
    public Future<?> submit(Runnable task) {
        JMVXJob wrapper = new JMVXJob(task);
        JMVXRuntime.submit(wrapper); //reuse code for now
        return super.submit(wrapper);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        JMVXJob wrapper = new JMVXJob(task);
        JMVXRuntime.submit(wrapper); //reuse code for now
        return super.submit(wrapper, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException("Haven't dealt with this case yet of submit yet");
        //I need a runnable for the beforeExecute. This isn't a runnable
        //return super.submit(task);
    }*/

    public void execute(JMVXJob command) {
        //already wrapped
        super.execute(command);
    }

    /*
     * There are many such methods to invoke an executor service. Implementing them all may be a pain
     * This is the method pmd uses
     */
    @Override
    public void execute(Runnable command) {
        //saturate the system
        if(saturate) {
            super.setCorePoolSize(super.getCorePoolSize() + 1);
        }
        JMVXJob wrapper = new JMVXJob(command, poolId, this);
        wrapper.$JMVX$setClock(clock.getAndIncrement());
        JMVXRuntime.submit(wrapper); //reuse code for now
        super.execute(wrapper);
    }

    public int getPoolId() {
        return poolId;
    }
}
