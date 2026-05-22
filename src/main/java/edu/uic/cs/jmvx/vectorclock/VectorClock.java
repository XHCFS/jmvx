package edu.uic.cs.jmvx.vectorclock;

import java.util.stream.LongStream;
import java.util.Arrays;

public class VectorClock {

    protected long[] clock = {0L};//new long[1]; //clinit slot is index 0
    public static final int START_INDEX = 1; //the index to start registering threads at
    private volatile long inClinit = -1L; //which thread is redirected to clinit slot

    /**
     * Returns either idx or 0 based on if the thread is currently in a static initializer (inClinit)
     * @param idx slot to use
     * @return idx or 0 (0 is the static initializer slot)
     */
    protected int confirmIndex(int idx){
        long tid = Thread.currentThread().getId();
        if(inClinit == tid){
            return 0;
        }
        return idx;
    }

    /**
     * Set if a thread is in a static initializer
     * Implicitly sync'ed because only 1 thread can run a static initializer at a time
     * @param val boolean if the thread is (or is not) in a static initializer
     */
    public void setInClinit(boolean val){
        long tid = Thread.currentThread().getId();
        if(val){
            inClinit = tid;
        }else{
            if(inClinit == tid)
                inClinit = -1L; //no thread in clinit
            else
                throw new Error("SHOULDN'T HAPPEN");
            //else someone else stole the value
        }
    }

    public synchronized int size() {
        return clock.length;
    }

    /**
     * Registers a new thread to the vector clock by concat'ing one slot
     * @return
     */
    public synchronized int registerNewThread(){
        //concat 1 to the clock
        return registerNewThread(this.clock.length);
    }

    /**
     * [Potentially] Expand the clock to support a slot at index idx
     * @param idx
     * @return
     */
    public synchronized int registerNewThread(int idx) {
        /*
        May want a fast path to determine if to update the VC for ReplayerWithPools
        in order to handle scaling thread pools
        Eg
        public int registerNewThread(int idx) {
          if(idx < this.clock.length)
            return idx
          else{
             synchronized(this){
               //increase size of clock (current method logic)
               //including checking idx against the size of the clock again
             }
          }
        }
         */
        //if we get an index that is larger than clock.length+1,
        //we will need to add arbitrarily many longs
        int min_length = idx + 1; //minimum length of the clock to manage threads 0 to idx
        if(min_length > this.clock.length) {
            long[] toConcat = new long[min_length - this.clock.length];
            Arrays.fill(toConcat, 0);
            clock = LongStream.concat(Arrays.stream(clock), LongStream.of(toConcat)).toArray();
        }
        return idx;
    }

    /**
     * Return a slot to the clock for rebinding/reuse
     * Does nothing here as this clock strategy always concats a new, unique slot
     * @param idx
     */
    public synchronized void unregisterThread(int idx){ }

    public synchronized long[] increment(int idx, long[] result) {
        idx = confirmIndex(idx);

        long val = clock[idx] + 1;
        clock[idx] = val;

        if (result != null) {
            if (result.length < clock.length)
                result = new long[clock.length];

            System.arraycopy(clock, 0, result, 0, clock.length);
        }

        return result;
    }

    public boolean sync(int idx, long[] clock) {
        return sync(idx, clock, false);
    }

    public boolean sync(int idx, long[] clock, boolean keepTrying) {
        idx = confirmIndex(idx);

        boolean yield = false;

        tryagain: while (true) {
            if(yield){
                Thread.yield();
                yield = false;
            }
            synchronized (this) {
                if (clock.length > this.clock.length) {
                    if (keepTrying) {
                        yield = true;
                        continue tryagain;
                    }else
                        return false;
                }

                for (int i = 0; i < clock.length; i++) {
                    if (idx == i) {
                        if (this.clock[i] + 1 != clock[i]) {
                            if (keepTrying) {
                                yield = true;
                                continue tryagain;
                            }else
                                return false;
                        }
                    } else if (this.clock[i] < clock[i]) {
                        /* Partially ordered events may run ahead. */
                        if (keepTrying) {
                            yield = true;
                            continue tryagain;
                        }
                        else
                            return false;
                    }
                }
                return true;
            }
        }
    }


    public long[] diffClock(long[] clock){
        long[] res = new long[this.clock.length];
        for(int i = 0; i < this.clock.length; i++){
            res[i] = clock[i] - this.clock[i];
        }
        return res;
    }
}
