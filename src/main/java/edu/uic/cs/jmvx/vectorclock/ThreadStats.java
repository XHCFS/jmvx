package edu.uic.cs.jmvx.vectorclock;

/*
A class for tracking sync rate statistics for the vector clock
 */
class ThreadStats {
    private long syncs = 0;
    private long backoffs = 1;
    //another thread beat us to making a decision and is processing this node
    //private volatile boolean processing = true;
    //if the data structure needs a reset
    //processing should be set to true first!
    //private volatile boolean reset = false;

    private static final int PADDING = 8;
    private static final int LINE = PADDING * Long.BYTES;
    // Preallocating memory for simplicity. This is the amount of entries to consider
    private static final int CAPACITY = BackoffVectorClock.SIZE;
    /* All the ThreadStats across all threads.
    The array is padded. I am assuming each pointer to an object is a long.
    The idea
    */
    private static ThreadStats[] stats = new ThreadStats[LINE * CAPACITY];

    static {
        //start with 1
        stats[0] = new ThreadStats();
    }

    private ThreadStats() {}

    private boolean canTrackUpdate() {
        /*if (this.processing) {
            if (!reset) {
                //someone else is processing us and hasn't finished
                return false;
            }
            //needs a state reset before tracking the update
            this.syncs = 0L;
            this.backoffs = 1L;
            this.processing = false;
            this.reset = false;
        }*/
        //can track update
        return true;
    }

    public long trackSync() {
        if (canTrackUpdate())
            syncs++;
        return syncs;
    }

    public long trackBackoff() {
        if (canTrackUpdate())
            backoffs++;
        return backoffs;
    }

    public long getSyncs() {
        return syncs;
    }

    public long getBackoffs() {
        return backoffs;
    }

    public long computeLocalStats(){
        return syncs / backoffs;
    }

    public static long computeGlobalStats() {
        long totalSyncs = 0;
        long totalBackoffs = 1;
        for(int i = 0; i < stats.length; i += LINE){
            ThreadStats s = stats[i];
            //we've hit the end of the active/registered threads
            if(s == null)
                break;
            /*
            Don't need the lock because the next entry is out of the cache line
            So attempting to read s means we need to read from main memory
            Also, if we read a stale value, it is okay.
            The results are out of date, but not incorrect.
             */
            //synchronized (s){
                totalSyncs += s.syncs;
                totalBackoffs += s.backoffs;
                //s.reset = true;
            //}
        }
        return totalSyncs / totalBackoffs;
    }

    public static void reset(){
        for(int i = 0; i < stats.length; i+=LINE){
            ThreadStats s = stats[i];
            if(s == null) {
                return;
            }else{
                s.syncs = 0;
                s.backoffs = 1;
            }
        }
    }

    private static int lastIdxInited = 0;

    public static synchronized void newThreadStats(int idx){
        //reuse already inited data
        if(idx <= lastIdxInited)
            return;
        for(int i = lastIdxInited + 1; i <= idx; i++) {
            ThreadStats newStats = new ThreadStats();
            stats[i * LINE] = newStats;
        }
        lastIdxInited = idx;
    }

    public static ThreadStats getThreadStats(int idx){
        return stats[idx * LINE];
    }
}
