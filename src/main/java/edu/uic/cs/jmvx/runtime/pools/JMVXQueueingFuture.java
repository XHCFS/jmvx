package edu.uic.cs.jmvx.runtime.pools;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
 */
public class JMVXQueueingFuture<V> implements Future<V>, JMVXOrderedEvent {
    private final JMVXCompletionServiceQueue<V> jmvxCompletionServiceQueue;
    private volatile int stamp = Integer.MAX_VALUE; //-1 puts unset stamps to the front!
    private final Future<V> future;

    public JMVXQueueingFuture(JMVXCompletionServiceQueue<V> jmvxCompletionServiceQueue, Future<V> task) {
        this.jmvxCompletionServiceQueue = jmvxCompletionServiceQueue;
        future = task;
    }

    @Override
    public void $JMVX$setClock(int newClock) {
        this.stamp = newClock;
    }

    /* set the clock to the next available value
     *  Used by the recorder */
    public int tick() {
        $JMVX$setClock(jmvxCompletionServiceQueue.clock.getAndIncrement());
        return $JMVX$getClock();
    }

    @Override
    public int $JMVX$getClock() {
        return this.stamp;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }
}
