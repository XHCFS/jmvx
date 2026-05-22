package edu.uic.cs.jmvx.runtime.pools;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;

import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/*
Optimized version of JMVXOrderedQueue for bound=1
 */
public class JMVXCompletionServiceQueue<V> extends PriorityBlockingQueue<Future<V>> {
    private volatile int nextStamp = 0; //volatile in case multiple threads attempt to use at once

    //used to order tasks completed
    protected AtomicInteger clock = new AtomicInteger(0);

    /*
    This runs before JMVXThreadPoolExecutor.afterExecute
     */
    @Override
    public boolean add(Future future) {
        JMVXQueueingFuture wrapped = new JMVXQueueingFuture(this, future);
        //the clock has to be set by the strategy! Done by the next call
        JMVXRuntime.runnableFutureDone(wrapped);
        return super.add(wrapped);
    }

    @Override
    public Future<V> take() throws InterruptedException {
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
        while (true) {
            JMVXQueueingFuture future = (JMVXQueueingFuture) super.take();
            if (future.$JMVX$getClock() == nextStamp) { //should run return
                nextStamp++;
                if (nextStamp == Integer.MAX_VALUE) {
                    throw new RuntimeException("Cannot track the order of futures anymore!");
                }
                return future;
            } else { //shouldn't run yet, put back
                super.add(future);
            }
        }
    }

    /*
    Like take but no delay, eg returns null if no items are present
     */
    @Override
    public Future<V> poll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
