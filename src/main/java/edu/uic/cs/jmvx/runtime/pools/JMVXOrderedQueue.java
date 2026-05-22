package edu.uic.cs.jmvx.runtime.pools;

import java.util.LinkedList;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
The code works, but has poor performance do to waste of CPU cores.
Scheduling a job early led to the queue considering more accurate stamps,
and left more cores open (not running tasks)

Before schedule
|--------|              scheduling interval
1  2  3  4  5  ...      tasks
c  c  c  c              fixed number of cores

Scheduling task 1 shortens the interval, and eventually expands it
|-----|xxx              scheduling interval
1  2  3  4  5  ...      tasks

When job 1 finsihes, the interval shifts and expands again
sss|--------|           scheduling interval
1  2  3  4  5  ...      tasks

The problem: priority queue will aim to schedule the lowest priority first,
and if task 1 is not available, task 2 is the worst for this approach to run

|--------|              scheduling interval
1  2  3  4  5  ...      tasks

If we start task 2 we eliminate the ability to start task 4, which should still be valid
|-----|xxx              scheduling interval
1  2  3  4  5  ...      tasks

A priority queue that adhere to the order of lamport clocks
The clock value is used as the priority.
The queue will return a clock such that the following relation is true:
event.stamp < lowestSequentialStamp + available

the lowest sequential stamp is the lowest clock value where all previous timestamps have been returned
The bound is set upon queue creation.
Available is a count of worker threads that are not actively running a job.
available < bound.
Bound is the size of the core pool.
A bound of 1 is strict, meaning that all events are returned in the order of the clocks.
A bound > 1 is loose, allowing some flexibility in the order.
This is used for thread pool job scheduling as
future jobs can be started, but a core pool thread will exist to run the earliest recorded job.
Ideally this will prevent deadlock, but I need to formalize this.
Symmetric execution will help with the proof (or at least convincing people)

Intuition:
As threads become available, we can start more jobs.
Eg if 2 threads are available when we hit the scheduler, we can be flexible with the scheduling.
The next 2 logically started threads can begin out of order or on different threads because
enough threads are available in the core pool.

Note:
This does not guarantee execution within a particular range of events.
If a task hangs (eg by losing a lock to other tasks), it is up to the vector clock
to ensure correct ordering.
The queue only assures that jobs are started in the same order, it does not order when they finish.
Returns of futures are handled by `JMVXCompletionService`.

Assumption for correctness:
The core pool starts with the first n jobs.
Eg a core pool of size 4 assumes the first 4 jobs have clocks 0,1,2,3
If this condition is violated, we deadlock.
Added coe in JMVXThreadPoolExecutor to add jobs to spawn core pool threads.
 */
public class JMVXOrderedQueue<V> extends PriorityBlockingQueue<V> {
    /* The lowest logical timestamp in which all prior events have been taken off the queue.
    Eg if this is k, then all i < k have been removed.
    lowestStamp is used to give us some room to take from.
     */
    private volatile int nextSequentialStamp;
    private volatile int available;
    private final int bound; //1 is strict, eg enforces exact ordering
    private final ThreadLocal<LinkedList<Integer>> startedOutofOrder = ThreadLocal.withInitial(() -> new LinkedList<>());

    public JMVXOrderedQueue(int bound) {
        this(0, bound);
        available = bound;
    }

    public JMVXOrderedQueue(int startingStamp, int bound) {
        super();
        /*
        We don't always start at 0
        Thread pools do not queue jobs UNTIL the core pool is created
        But the core pools is spawned lazily.
        Eg the jobs that spawn core pool threads NEVER HIT THE QUEUE
        So our first job happens when all core threads are in use
         */
        nextSequentialStamp = startingStamp;
        available = 0;
        this.bound = bound;
    }

    @Override
    public V poll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public V poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public V take() throws InterruptedException {
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
            V item = super.take();
            JMVXOrderedEvent event = (JMVXOrderedEvent) item;
            int stamp = event.$JMVX$getClock();
            /*
            The available field is interesting
            It makes the job scheduling more "precise" as we schedule things out of order
            Which is nice, but it does not take advantage of the pool completely.
            Eg scheduling (nextStamp + 1) eliminates the possibility of scheduling (nextStamp + bound - 1), even though
            that job is technically valid.
            The queue fights us, if we don't get the next sequential stamp, we get the worst possible job to start.
            However, tracking available cores is needed for correctness.
            Imagine a program with 100 tasks, but the first one lives for the whole program.
            Having the available field allows us to ensure our ordering constraint.
            If we just used the bound, its possible we start a shorter lived job our of order.
            Eg with 4 cores, if we just look at bound:
            Task 0 is running on core 1 all program
            [nextStamp = 1, bound = 4, anything less than 5 can be started)
            tasks 2,3,4 get started on the remaining cores
            task 1 is skipped over.
            With available, only anything less than 4 can be started,
            so task 1 is never skipped
             */
            if(stamp == nextSequentialStamp){
                //new task
                available--;
                nextSequentialStamp++;
                return item;
            }else if(stamp < nextSequentialStamp + available) {
                available--;
                /*
                preemptively return this value, because it is in range
                track stamp was started out of order so next sequential stamp can be updated later
                 */
                startedOutofOrder.get().add(stamp);
                return item;
            } else{
                //put back and try again
                super.add(item);
                /*check if this next sequential stamp was started out of order
                For lots of short jobs, it was better to have this code here than at the beginning of the method.
                 */
                if(startedOutofOrder.get().removeFirstOccurrence(nextSequentialStamp)){
                    nextSequentialStamp++;
                }
            }
        }
    }

    public void finishTask(){
        available++;
        //correctness condition
        if(available > bound)
            throw new Error("Cannot have more available threads than those in the core pool");
    }
}
