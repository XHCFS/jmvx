package edu.uic.cs.jmvx.vectorclock;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;

import java.util.Optional;

/**
 * Implements similar logic to the BackoffVectorClock, but uses signals (via wait/notifyAll)
 * instead of spinning in a local cache line.
 * Designed to assist with divergent scheduling during replay of thread pools.
 *
 * A divergent task schedule may lead to deadlock.
 * onSuspendHook runs when a thread goes to suspend itself
 * It is used by OptimisticExecutor to spawn additional tasks out of band to break out of deadlock
 */
public class WaitNotifyVectorClock extends VectorClock {
    private final static int LONG_BYTES = 8;
    /*
    Partial order implementation may get in the way of waiting
    Some slots may run ahead
    because partially ordered events increment their slot without a sync.
    This will reduce the total different between our expected clock and the program clock.
     */
    private int waitIfOver = 8;
    private Optional<Runnable> onSuspendHook = Optional.empty();

    private Object[] notifiers = new Object[64];

    public WaitNotifyVectorClock() {
        super();
        for(int i = 0; i < notifiers.length; i++){
            notifiers[i] = new Object();
        }
    }

    @Override
    public synchronized int registerNewThread(int idx) {
        if(idx + 1 >= notifiers.length) {
            throw new RuntimeException("WaitNotifyVectorClock does not have room for another thread");
        }
        return super.registerNewThread(idx);
    }

    @Override
    public long[] increment(int idx, long[] result) {
        idx = confirmIndex(idx);
        if (result == null) {
            // This is the follower
            // Increment my position
            long val = JMVXRuntime.unsafe.getLongVolatile(clock, CircularBuffer.longOffset + idx* LONG_BYTES) + 1;
            JMVXRuntime.unsafe.putLongVolatile(clock, CircularBuffer.longOffset + idx * LONG_BYTES, val);

            // Is anyone waiting for me?
            Object lock = notifiers[idx];
            synchronized (lock) {
                lock.notifyAll();
                return result;
            }
        } else {
            synchronized (this) {
                // This is the leader
                long val = clock[idx] + 1;
                // Make sure to use a volatile write, so other threads (not using synchronized) can see it
                JMVXRuntime.unsafe.putLongVolatile(clock, CircularBuffer.longOffset + idx * LONG_BYTES, val);

                if (result != null) {
                    if (result.length < clock.length)
                        result = new long[clock.length];

                    JMVXRuntime.unsafe.copyMemory(clock, CircularBuffer.longOffset, result, CircularBuffer.longOffset, clock.length * LONG_BYTES);
                }

                return result;
            }
        }
    }

    @Override
    public boolean sync(int idx, long[] clock, boolean keepTrying) {
        idx = confirmIndex(idx);
        boolean yield = false;

        while (true) {
            synchronized (this) {
                if (clock.length > this.clock.length) {
                    if (keepTrying) {
                        Thread.yield();
                        continue;
                    } else
                        return false;
                }
                break;
            }
        }

        long max = 0; //delta of most distant slot
        long total = 0; //total difference in clocks
        int ii  = 0; //index of most distant slot
        int n   = 0; //index of clock to check

        tryagain: while (true) {
            if(yield){
                // We had to wait
                if (total > waitIfOver) {
                    // We have fallen behind, wait on the most distant thread, similar to BackoffVectorClock
                    Object lock = notifiers[ii];
                    synchronized (lock){
                        try {
                            onSuspendHook.ifPresent((hook) -> hook.run());
                            /*
                            timed wait, so we recheck the clcok after 750 ms
                            saves us in case we miss a signal
                            eg we were not in the wait set for the thread we cared about when the signal was sent out
                            so we missed it. Note: this timeout is long.
                            We see partial order improvements (with serialized objects)
                            with BackOffVectorClock but not WaitNotify due to poor choice of timeout.
                             */
                            lock.wait(750);
                        } catch (InterruptedException e) {
                            //e.printStackTrace();
                        }
                        //normally need to reacquire lock, but we would just release it anyway
                        //should we wake up other threads who may be waiting for us here?
                    }
                }
                else {
                    // Someone might be waiting for us, let them go
                    Object lock = notifiers[idx];
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                    // We're not that far behind, yield and try again
                    Thread.yield();
                }
                yield = false;
            }

            //see declaration for breakdown of vars
            max = 0;
            ii  = 0;
            n   = 0;
            total = 0;
            // Compute how far we are from everyone else
            for (int i = 0; i < clock.length; i++) {
                if (i == idx)
                    continue;
                long c = JMVXRuntime.unsafe.getLongVolatile(this.clock, CircularBuffer.longOffset + i*LONG_BYTES);
                //c > clock if it has run partial order events.
                long diff = clock[i] - c;

                // Are we caught up?
                if (diff > 0) {
                    total += diff;
                    // No, compute how far we are
                    n += 1;
                    if (diff > max) {
                        max = diff;
                        ii = i;
                    }
                }
            }

            // Are we up to date?
            if (n == 0 && JMVXRuntime.unsafe.getLongVolatile(this.clock, CircularBuffer.longOffset + idx*LONG_BYTES) + 1 == clock[idx])
                return true;
            else if (keepTrying) {
                yield = true;
                continue;
            } else {
                return false;
            }
        }
    }

    public int getWaitIfOver() {
        return waitIfOver;
    }
    public void setWaitIfOver(int waitIfOver) {
        this.waitIfOver = waitIfOver;
    }
    public void setOnSuspendHook(Runnable onSuspendHook) {
        this.onSuspendHook = Optional.ofNullable(onSuspendHook);
    }
}
