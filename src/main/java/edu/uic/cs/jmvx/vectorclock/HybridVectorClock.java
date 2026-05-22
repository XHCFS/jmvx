package edu.uic.cs.jmvx.vectorclock;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;

import java.util.concurrent.atomic.AtomicInteger;

public class HybridVectorClock extends BackoffVectorClock {

    public final static boolean reuseSlots = JMVXRuntime.parameters.reuseVCSlots;

    private volatile boolean spin = true;
    private Object[] notifiers = new Object[BackoffVectorClock.SIZE]; //need padding?

    /* Decider is a consensus value. It prevents other threads from
     * attempting to change the strategy when one thread is already making the decision
     * or actively changing the strategy. */
    private AtomicInteger decider = new AtomicInteger(-1);

    /** Stores reusable slots in the vector clock
     * Goal is to keep the size of the clock minimal to reduce IO overhead on the slow path
     * For now, has a constant maximum size because other optimized portions of the vector clock have size limits
     * But this could grow with the clock itself.
     */
    ConstantSizeMinHeap reusableSlots = new ConstantSizeMinHeap(BackoffVectorClock.SIZE);


    private final long GOTO_WAIT = 2L;
    private final long GOTO_SPIN = 8L;

    public HybridVectorClock() {
        super();
        for (int i = 0; i < notifiers.length; i++) {
            notifiers[i] = new Object();
        }
    }

    @Override
    protected boolean isAnotherThreadWaiting(int idx, long val) {
        if (spin) {
            return super.isAnotherThreadWaiting(idx, val);
        } else {
            return true;
        }
    }

    @Override
    protected void signalWaiting(int idx) {
        if (spin) {
            super.signalWaiting(idx);
        } else {
            // Is anyone waiting for me?
            Object lock = notifiers[idx];
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    @Override
    protected void backoff(int idx, long[] clock, int ii) {
        trackBackoff(idx);
        if (spin) {
            super.backoff(idx, clock, ii);
        } else {
            // We have fallen behind, wait on the most distant thread, similar to BackoffVectorClock
            Object lock = notifiers[ii];
            synchronized (lock) {
                try {
                    /*
                    timed wait, so we recheck the clcok after 750 ms
                    saves us in case we miss a signal
                    eg we were not in the wait set for the thread we cared about when the signal was sent out
                    so we missed it. Note: this timeout is long.
                    We see partial order improvements (with serialized objects)
                    with BackOffVectorClock but not WaitNotify due to poor choice of timeout.
                    */
                    lock.wait(150);
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                }
                //normally need to reacquire lock, but we would just release it anyway
                //should we wake up other threads who may be waiting for us here?
            }
        }
    }

    //for leader
    @Override
    public synchronized int registerNewThread() {
        /* this implementation allows the min heap to grow with the clock,
         * as opposed to pre-populating all the possible slot indices
         *
         * Note: if reuseSlots = false, the min heap is always empty,
         * and we always allocate new slots via super calls
         */
        int idx;
        if(reusableSlots.isEmpty()){
            //no reusable slots are available, need to allocate more space
            idx = super.registerNewThread();
        }else{
            //get the lowest reusable index to minimize clock size
            idx = reusableSlots.poll();
            super.registerNewThread(idx);
        }
        ThreadStats.newThreadStats(idx);
        return idx;
    }

    //for follower
    @Override
    public synchronized int registerNewThread(int idx) {
        /* Right now, we skip this because the follower/replayer don't need to manage the usableSlots
         * as they get their binding from log files.
         * TODO: the follower SHOULD manage this to support role swap.
         */
        //usableSlots.remove(idx);
        ThreadStats.newThreadStats(idx);
        return super.registerNewThread(idx);
    }

    //for leader when a thread exits, return the id bound
    @Override
    public synchronized void unregisterThread(int idx){
        if(reuseSlots)
            reusableSlots.offer(idx);
    }

    private void trackBackoff(int idx) {
        ThreadStats.getThreadStats(idx).trackBackoff();
    }

    @Override
    public boolean sync(int idx, long[] clock, boolean keepTrying) {
        /* change strategy
         * We should be able to get away with this because backoffs naturally end after a timeout or max iters
         * NOTE: I should probably enforce that in the code with enterBackoff/canExitBackoff
         * then i can have the max iters part of the loop concrete in backoff
         * what if the threads tracked these stats on their own until 1 thread hit a decision point?
         * then we coalescence the results and make a decision based on local decisions
         * Also, waiting for the backoff to boot out might be a bad idea
         */
        Long sp = trackSync(idx);
        if (considerUpgrade(sp, idx) && decider.compareAndSet(-1, idx)) {
            /*if(maybeUpgrade(sp)){
                ThreadStats.reset();
            }*/
            maybeUpgrade(sp);
            decider.set(-1); //never go back
        }
        return super.sync(idx, clock, keepTrying);
    }

    private boolean maybeUpgrade(Long sp) {
        long rate = ThreadStats.computeGlobalStats();
        if (spin) {
            if (rate <= GOTO_WAIT) {
                spin = false;
                return true;
            }else{
                //else stay on current strategy
                return false;
            }
        } else {
            if (rate >= GOTO_SPIN) {
                spin = true;
                return true;
            }else {
                //else stay on current strategy
                return false;
            }
        }
    }

    private boolean considerUpgrade(Long sp, int idx) {
        /*if (ThreadStats.getThreadStats(idx).getSyncs() > nextDecisionAt) {
            nextDecisionAt *= 2;
            return true;
        } else {
            return false;
        }*/
        long localRate = ThreadStats.getThreadStats(idx).computeLocalStats();
        if (spin) {
            if (localRate <= GOTO_WAIT)
                return true;
            //else stay on current strategy
        } else {
            if (localRate >= GOTO_SPIN)
                return true;
            //else stay on current strategy
        }
        return false;
    }

    private Long trackSync(int idx) {
        return ThreadStats.getThreadStats(idx).trackSync();
    }
}