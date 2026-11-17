package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.runtime.JMVXMonitor;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;
import edu.uic.cs.jmvx.runtime.util.UIDedObject;
import edu.uic.cs.jmvx.vectorclock.VectorClock;

public class RecorderRTOArtifactTracer extends RecorderWithPools {

    private static Object ePrev = null;
    private static long artifactCount = 0;
    private static long firstUseCount = 0;
    private static long totalCount = 0;
    private static long nonrelaxed = 0;
    //private static long distanceSum = 0;
    //private static long distances = 0;

    /* NOTE: NOT THREAD SAFE
    *  This counts both FIRST USE and TO ARTIFACTS */
    private boolean countArtifact(Object o){
        nonrelaxed++;
        boolean artifact = ePrev != o;
        //update for next pass
        ePrev = o;
        if(artifact){
            artifactCount++;
        }
        return artifact;
    }

    @Override
    protected boolean canTakeClockFastPath(Object o){
        /* partial order optimization during recording
         * the first use of a monitor gets the fast path
         * if the owner of a monitor has not changed, then use the fast path
         *
         * Slow path will force a sync on the replayer when the monitor changes owners
         * OR if the object belongs to java.lang (eg is not an instance of UIDedObject)
         *
         * Note: this method assumes that you have grabbed the object monitors and are checking
         * against the prior owner. This assumption allows us to be STATEFUL
         * the last owner is replaced
         * */
        if(computePartialOrders && o instanceof UIDedObject) {
            UIDedObject u = (UIDedObject) o;
            Object priorOwner = u.getLastOwner();
            Object ownerRef = getMonitorOwnerReference();
            //priorOwner==null means no one owned the monitor before
            if (priorOwner == ownerRef) {
                return true; //can take the fast path
            }else{
                if(priorOwner == null)
                    firstUseCount++;
                u.setLastOwner(ownerRef);
            }
        }
        return false; //cannot take the fast path
    }

    private static long sumClock(long[] clk){
        long sum = 0;
        for(long l: clk){
            sum += l;
        }
        return sum;
    }

    /* Requires a modification to UIDedObject/OwnedObject to work
    private boolean countDistance(Object o, long[] clockCopy){
        if(o instanceof UIDedObject){
            UIDedObject u = (UIDedObject) o;
            long sequenceNumber = sumClock(clockCopy);
            long priorSequenceNumber = u.getLastSequenceNumber();
            //was a first use, so we don't care about the artifacting
            boolean computeDistance = priorSequenceNumber > 0;
            if(computeDistance){
                long distance = sequenceNumber - priorSequenceNumber;

                //check for overflow
                long temp = distanceSum;
                if(distanceSum < temp){
                    log.error("Overflow in distanceSum");
                    throw new RuntimeException("Overflow in distanceSum!");
                }

                //update totals
                distanceSum += distance;
                distances++;
            }

            //set for next iter
            u.setLastSequenceNumber(sequenceNumber);

            return computeDistance;
        }else{
            return false;
        }
    }*/

    /* NOTE: monitorenter and wait are copied code from Recorder (and RecorderWithPools)
             plus an additional synchronization to detect RTO artifacting
             e.g., when we add a total order edge to synchronize on ei-1 instead of e_prev
     */
    @Override
    public void monitorenter(Object o) {
        assert inClinit == 0;

        if (inClinit > 0 || loading > 0) {
            // Don't log monitors inside class initializers
            // These should be able to be run by any thread
            // Just get the lock
            JMVXMonitor.enter(o);
            return;
        }

        errIfWorkerWithoutTask();

        JMVXMonitor.enter(o);
        /* The clock needs to be incremented no matter what
         * The replayer needs to know how long to wait while on the slow path
         * Incrementing on the fast path performs this bookkeeping
         * The increments logs "how many [fast] operations do other threads need to wait for"
         * Without the increment, the replayer may preempt fast path ops with slow path ones
         * which should take place later */
        VectorClock clock = JMVXRuntime.clock;
        /* IMPORTANT DISTINCTION FROM THE LEADER HERE
         *  clockCopy and newClockCopy may be different references
         *  Detecting that in writeClockSlowPath adjusts the `bytes` field which is used for encoding the clock
         *  The Leader immediately changes the clockCopy reference because it can write long[] to the
         *  circular buffer fine.
         *
         *  Added a synchronized block to atomicize fastpath, artifacting, and increment*/
        synchronized (clock) {
            totalCount++;
            if (canTakeClockFastPath(o)) {
                clock.increment(vectorClockIndex, null); //skip the array copy
                writeClockFastPath(OptimizedEvent.FAST_MONITOR_ENTER);
            } else {
                countArtifact(o);
                long[] newClockCopy = clock.increment(vectorClockIndex, clockCopy);
                //countDistance(o, clockCopy);
                writeClockSlowPath(OptimizedEvent.MONITOR_ENTER, newClockCopy);
            }
        }
    }

    @Override
    public void wait(Object o, long timeout, int nanos) throws InterruptedException {
        //VectorClock clock = JMVXRuntime.clock;
        o.wait(timeout, nanos); //what happens if we timeout or wakeup spuriously?
        if (inClinit > 0 || loading > 0) {
            // Don't log monitors inside class initializers
            // These should be able to be run by any thread
            // Just get the lock
            return;
        }

        errIfWorkerWithoutTask();

        /* The clock needs to be incremented no matter what
         * The replayer needs to know how long to wait while on the slow path
         * Incrementing on the fast path performs this bookkeeping
         * The increments logs "how many [fast] operations do other threads need to wait for"
         * Without the increment, the replayer may preempt fast path ops with slow path ones
         * which should take place later */
        VectorClock clock = JMVXRuntime.clock;
        synchronized (clock) {
            totalCount++;
            if (canTakeClockFastPath(o)) {
                clock.increment(vectorClockIndex, null); //skip the array copy
                writeClockFastPath(OptimizedEvent.FAST_WAIT);
            } else {
                countArtifact(o);
                long[] newClockCopy = clock.increment(vectorClockIndex, clockCopy);
                //countDistance(o, clockCopy);
                writeClockSlowPath(OptimizedEvent.WAIT, newClockCopy);
            }
        }
    }

    private static void reportProportion(long count, String countName, long total, String totalName){
        double proportion = (double) count / (double) total;
        System.out.format("%s / %s : %d / %d = %.3f\n", countName, totalName, count, total, proportion);
    }

    private static void reportAverage(long sum, long n, String name){
        double avg = (double) sum / (double) n;
        System.out.format("Avg %s : %.3f\n", name, avg);
    }

    @Override
    public void exitLogic() {
        //weird couldn't override flushData, maybe because its static?
        //so we copied the implementation from Recorder
        if (!Thread.currentThread().getName().equals(JMVXRuntime.mainThread.getName()))
            return;

        try{
            JMVXRuntime.enter();
            synchronized (RecorderRTOArtifactTracer.class) {
                reportProportion(nonrelaxed, "Nonrelaxed", totalCount, "Monitor events");
                reportProportion(artifactCount, "Artifacts", nonrelaxed, "Nonrelaxed");
                reportProportion(firstUseCount, "First Uses", artifactCount, "Artifacts");
                //reportAverage(distanceSum, distances, "Observable Artifact Distance");
            }
        }finally {
            JMVXRuntime.exit();
        }

        //and have it call back to its super
        super.exitLogic();
    }
}
