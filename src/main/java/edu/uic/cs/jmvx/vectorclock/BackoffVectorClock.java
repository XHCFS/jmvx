package edu.uic.cs.jmvx.vectorclock;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;

public class BackoffVectorClock extends VectorClock {
    public final static int SIZE = 100;
    private final static int PADDING = 8;
    private final static int LONG_BYTES = 8;

    private long[] backoff = new long[SIZE * PADDING];

    protected boolean isAnotherThreadWaiting(int idx, long val){
        long c = JMVXRuntime.unsafe.getLongVolatile(backoff, CircularBuffer.longOffset + ((idx * LONG_BYTES) * PADDING));
        return c != 0 && c <= val;
    }

    protected void signalWaiting(int idx){
        JMVXRuntime.unsafe.putLongVolatile(backoff, CircularBuffer.longOffset + ((idx * LONG_BYTES) * PADDING), 0);
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
            if(isAnotherThreadWaiting(idx, val))
                // Yes, let them go
                signalWaiting(idx);

            return result;
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

        long max = 0;
        int ii  = 0;
        int n   = 0;

        tryagain: while (true) {
            if(yield){
                // We had to wait
                if (max > 1_000) {
                    backoff(idx, clock, ii);
                }
                else {
                    // Someone might be waiting for us, let them go
                    signalWaiting(idx);
                    // We're not that far behind, yield and try again
                    Thread.yield();
                }
                yield = false;
            }

            max = 0;
            ii  = 0;
            n   = 0;
            // Compute how far we are from everyone else
            for (int i = 0; i < clock.length; i++) {
                if (i == idx)
                    continue;
                long c = JMVXRuntime.unsafe.getLongVolatile(this.clock, CircularBuffer.longOffset + i*LONG_BYTES);
                long diff = clock[i] - c;
                // Are we caught up?
                if (diff > 0) {
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

    protected void backoff(int idx, long[] clock, int ii) {
        // We have fallen behind, go to backoff array to avoid cache coherence traffic
        // First, ask to be notified when the most distant thread gets to where we need to be
        //enterBackoff(ii);
        JMVXRuntime.unsafe.putLongVolatile(this.backoff, CircularBuffer.longOffset + (ii * LONG_BYTES) * PADDING, clock[ii]);
        //max iters we wait until we force a second check of the clock
        //saves us in case of a missed update
        int iters = 0;
        // Then, wait to be notified.  This loop should happen inside our own cache line, and generate no cache coeherence traffic
        while (iters < 10_000 && JMVXRuntime.unsafe.getLongVolatile(this.backoff, CircularBuffer.longOffset + (ii * LONG_BYTES) * PADDING) == clock[ii]){
            // Someone might be waiting for us in the meantime, let them go
            signalWaiting(idx);
            iters++;
        }
    }
}
