package edu.uic.cs.jmvx.runtime.pools;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class PessimisticExecutor extends JMVXThreadPoolExecutor{
    public PessimisticExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new SameOrderQueue(), threadFactory);
        for(int i = corePoolSize; i > 0; i -= prestartAllCoreThreads());
    }

    /*
    Have some code duplication, but it makes it easier for the types to work out
    JMVXThreadPoolExecutor handles wrapping the data when the job is submitted
    also handles setting the job's stamp.
    So we just need to override the take method.
    This code does the same as JMVXCompletionServiceQueue
     */
    public static class SameOrderQueue extends PriorityBlockingQueue<Runnable> {
        private volatile int nextStamp = 0; //volatile in case multiple threads attempt to use at once

        @Override
        public Runnable take() throws InterruptedException {
            /*
            Peek doesn't resort the queue
            Poll does, but removes the head
            If we get something out of order, put back and retry

            Poll/replace works better than iterating over elements in the queue to check
            that the correct future was there and then performing 1 poll
            Saved ~1 sec on pmd in both Recorder/Replayer (on a macbook running w/ 4 threads)

            Both recorder/replayer are affected by this.
            This method is likely called on a different thread
            than the task was run on.
            Eg in pmd, `main` threads calls this vs any worker thread.
            So it would be difficult to update the task's log with the
            stamp value to place in the future.
            In theory asymmetric computation would speed up the Recorder.
            Eg recorder accesses the clock here, whereas the replayer sets it earlier.
             */
            while(true) {
                JMVXJob event = (JMVXJob) super.take();
                if(event.$JMVX$getClock() == nextStamp){ //should run return
                    nextStamp++;
                    return event;
                }else{ //shouldn't run yet, put back
                    super.add(event);
                }
            }
        }

        /*
        Like take but no delay, eg returns null if no items are present
         */
        @Override
        public Runnable poll() {
            try {
                return poll(0, TimeUnit.NANOSECONDS);
            }catch (InterruptedException _) {
                return null;
            }
        }

        @Override
        public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
            /*eventually, we'll time out
            * Is that desirable? Cannot guarantee that we didn't time out because
            * the job we were waiting hasn't come in yet. */
            while(true) {
                JMVXJob event = (JMVXJob) super.poll(timeout, unit);
                if(event == null) { //timeout
                    return null;
                } else if (event.$JMVX$getClock() == nextStamp) { //should run
                    nextStamp++;
                    return event;
                } else { //shouldn't run yet, put back
                    super.add(event);
                }
            }
        }
    }
}
