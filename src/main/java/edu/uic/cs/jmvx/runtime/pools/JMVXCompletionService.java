package edu.uic.cs.jmvx.runtime.pools;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class JMVXCompletionService<V> extends ExecutorCompletionService<V> {

    public JMVXCompletionService(Executor executor) {
        this(executor, null);
    }

    public JMVXCompletionService(Executor executor, BlockingQueue<Future<V>> completionQueue) {
        /*
        eventually will need to modify priority queue will be subbed out for one which forces elements to be successors
        eg if the queue is full of [1, 2, 3] and is set to start from priority 0, it will spin until 0 is added
        so that it can return 0 first.
        This ensures correct replay (as we race to add items to this queue)
         */
        super(executor, new JMVXCompletionServiceQueue());
    }

    @Override
    public Future<V> submit(Callable<V> task) {
        return super.submit(task);
    }

    @Override
    public Future<V> submit(Runnable task, V result) {
        return super.submit(task, result);
    }

    /*public static class JMVXCompletionServiceQueue2<V> extends JMVXOrderedQueue<java.util.concurrent.Future<V>> {
        public JMVXCompletionServiceQueue2() {
            super(1); //strict
        }

        @Override
        public boolean add(Future future) {
            JMVXQueueingFuture wrapped = new JMVXQueueingFuture(future);
            JMVXRuntime.runnableFutureDone(wrapped);
            return super.add(wrapped);
        }
    }*/


}
