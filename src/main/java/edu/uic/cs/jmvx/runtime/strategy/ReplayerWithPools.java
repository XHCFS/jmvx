package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.runtime.DivergenceError;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.pools.*;
import edu.uic.cs.jmvx.runtime.util.recording.ReadReplayerUtility;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.*;

public class ReplayerWithPools extends Replayer {

    private ObjectInputStream workerBackup = null;
    int oldId = -1;
    private boolean inTask = false;
    private boolean workerThread = false;
    private int taskID = -1;
    /* Set to true if a cached thread pool has ever been created
    * It means we need to double check the number of slots in the vector clock when
    * starting a new task*/
    private static volatile boolean cachedThreadPoolExists = false;

    @Override
    public void main() {
        workerThread = Thread.currentThread() instanceof JMVXThreadFactory.JMVXPoolThread;
        if(workerThread){
            super.log = Logger.getLogger(ReplayerWithPools.class);
            readReplayer = new ReadReplayerUtility(avoidObjectSerialization, circularBufferCommunicationEnabled);
        }else {
            super.main();
        }
    }

    @Override
    public void beforeExecute(Thread t, JMVXJob r) {
        try {
            //the vector clock is not configured correctly!
            //we need to add to the clock if we find an index we can't sync on!
            JMVXRuntime.enter();
            workerBackup = fromCoordinator;
            Path toTaskLog = RecorderWithPools.getJobPath(r);
            String fname = toTaskLog.toString();
            taskID = r.$JMVX$getClock();
            fromCoordinator = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fname)));
            oldId = vectorClockIndex;
            vectorClockIndex = fromCoordinator.readInt();
            /* Safety net for cached thread pools
            * A cached thread pool starts additional threads as needed
            * At the start of every task, we check that the vector clock is properly configured
            * In the presence of a fixed thread pool, the slots are created during the thread pools construction
            * TODO: measure the impact of this check (on PMD)*/
            if(cachedThreadPoolExists)
                JMVXRuntime.clock.registerNewThread(vectorClockIndex);
        }catch (IOException e) {
            //we're doomed, log not found or we failed to read it
            throw new Error(e);
        }finally {
            JMVXRuntime.exit();
        }
    }

    @Override
    public void afterExecute(JMVXJob r, Throwable t) {
        assert oldId != -1;
        try {
            fromCoordinator.close();
        }catch (IOException e){
            throw new Error(e);
        }
        fromCoordinator = workerBackup;
        vectorClockIndex = oldId;
        oldId = -1;
    }

    @Override
    public void submit(JMVXJob task) {
        Object read = receiveObjectFromCoordinator();
        if(read instanceof RecorderWithPools.JobCreated) {
            task.$JMVX$setClock(((RecorderWithPools.JobCreated)read).id);
        }else{
            throw new DivergenceError();
        }
    }

    @Override
    public void runnableFutureDone(JMVXQueueingFuture future) {
        Object read = receiveObjectFromCoordinator();
        if(read instanceof RecorderWithPools.TaskFinished){
            future.$JMVX$setClock(((RecorderWithPools.TaskFinished)read).stamp);
        }else{
            throw new DivergenceError();
        }
    }

    @Override
    public ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
        /*
        Breakdown:
        Pessimistic - forces jobs to be started in the same order, but the task-thread mapping can be different
        Optimistic  - Jobs can start in different order in replay,
                      but will spawn additional tasks if the system gets stuck
        */
        //preregister needed slots and avoid extra synchronzied method calls later
        synchronized (JMVXRuntime.clock) {
            JMVXRuntime.clock.registerNewThread(JMVXRuntime.clock.size() + nThreads + 1);
        }
        /*if(JMVXRuntime.clock instanceof WaitNotifyVectorClock) {
            WaitNotifyVectorClock clk = (WaitNotifyVectorClock) JMVXRuntime.clock;
            clk.setWaitIfOver(nThreads + 1);
        }*/
        /*return new OptimisticExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));*/
        //pessimistic pool enforces ordering (monotomically increasing job id)
        return new PessimisticExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
    }

    @Override
    public ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        cachedThreadPoolExists = true;
        return new PessimisticExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
    }
}
