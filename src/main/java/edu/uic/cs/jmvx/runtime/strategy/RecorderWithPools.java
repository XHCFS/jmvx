package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.runtime.*;
import edu.uic.cs.jmvx.runtime.pools.*;
import edu.uic.cs.jmvx.runtime.util.recording.ReadRecorderUtility;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class RecorderWithPools extends Recorder{

    //we cannot have multiple workers on the same thread
    //so we just have a single ref
    private ObjectOutputStream workerBackup = null; //holds a ref to old toCoordinator stream while a worker runs
    private boolean workerThread = false;

    @Override
    public void main() {
        workerThread = Thread.currentThread() instanceof JMVXThreadFactory.JMVXPoolThread;
        //Thread.currentThread().getName().contains("Worker");
        /*
        Main will add this thread to the vector clock AND open a log file
        We don't want a log file for the worker thread, because it does nothing
        Instead, we'll open logfiles for tasks
        Note: we do this similar to ReentrantStrategy,
        where we check that a worker is running a task before each event
        So we know if this assumption (that workers never log anything) is violated.
         */
        if(workerThread) {
            //only part of main we wish to execute
            //Not effected by parameters.reuseVCSlots because older versions of JMVX fixed the task -> thread
            //binding in order to reuse the slot implicitly (as opposed to rebinding slots in the log).
            vectorClockIndex = clockIndex.getAndIncrement();
            vectorClockIndex = JMVXRuntime.clock.registerNewThread();
            readRecorder = new ReadRecorderUtility(avoidObjectSerialization, circularBufferCommunicationEnabled, false);
        }else{
            super.main();
        }
    }

    /* doesn't alter the vector clock */
    @Override
    public void main(int clockIndex, boolean makeStream){
        workerThread = Thread.currentThread() instanceof JMVXThreadFactory.JMVXPoolThread;
        //Thread.currentThread().getName().contains("Worker");
        /*
        Main will add this thread to the vector clock AND open a log file
        We don't want a log file for the worker thread, because it does nothing
        Instead, we'll open logfiles for tasks
        Note: we do this similar to ReentrantStrategy,
        where we check that a worker is running a task before each event
        So we know if this assumption (that workers never log anything) is violated.
         */
        if(workerThread) {
            //only part of main we wish to execute
            vectorClockIndex = clockIndex;//clockIndex.getAndIncrement();
            //JMVXRuntime.clock.registerNewThread(vectorClockIndex);
        }else{
            super.main(clockIndex, makeStream);
        }
    }

    public static Path getPoolPath(JMVXThreadPoolExecutor pool){
        return Paths.get(recordingDir,
                String.format(Locale.US, "pool-%d", pool.getPoolId()));
    }

    public static Path getPoolPath(JMVXJob jobInPool){
        return Paths.get(recordingDir,
                String.format(Locale.US, "pool-%d", jobInPool.getPoolId()));
    }

    public static Path getJobPath(JMVXJob job){
        return getPoolPath(job).resolve(String.format("recording.worker%d.dat", job.$JMVX$getClock()));
    }

    private void createPoolRecordingDir(JMVXThreadPoolExecutor pool){
        try {
            JMVXRuntime.enter();
            File poolDir = getPoolPath(pool).toFile();
            if (!poolDir.mkdir())
                log.warn("Failed to make recording directory for pool");
        }finally {
            JMVXRuntime.exit();
        }
    }

    @Override
    public ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
        /*JMVXThreadPoolExecutor pool = new JMVXThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));*/
        JMVXThreadPoolExecutor pool = new PessimisticExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
        createPoolRecordingDir(pool);
        return pool;
    }

    @Override
    public ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        //why not a pessimistic executor? b/c the scheduler isn't used?
        JMVXThreadPoolExecutor pool = new JMVXThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
        createPoolRecordingDir(pool);
        return pool;
    }

    @Override
    public void beforeExecute(Thread t, JMVXJob r) {
        //we cannot switch between workers at this moment
        assert workerBackup == null;
        String fname = getJobPath(r).toString();
        try {
            JMVXRuntime.enter();
            ObjectOutputStream newStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fname)));
            //Paths.get(recordingDir, fname).toString())));
            newStream.writeInt(vectorClockIndex);
            newStream.flush();
            streamsToFlush.put(fname, newStream);
            workerBackup = toCoordinator; //need to preserve this
            toCoordinator = newStream;
            //log.info(String.format("TASK %d, IDX %d", r.$JMVX$getClock(), vectorClockIndex));
        } catch (IOException e) {
            throw new Error(e);
        } finally {
            JMVXRuntime.exit();
        }
        //inTask = true;
        monitorOwnerRef = r;
    }

    @Override
    public void afterExecute(JMVXJob r, Throwable t) {
        //have to have a stream to restore
        assert workerBackup != null;
        //super.afterExecute(r, t);
        //stream we "lose" was already added to streamsToFlush
        //and is handled later
        toCoordinator = workerBackup;
        workerBackup = null;
        //inTask = false;
        monitorOwnerRef = this;
    }

    /**
     * Throws an UnsupportedOperationException if called from a worker thread
     * that is not currently running a task
     *
     * Working on the assumption right now that workers do not need to be logged
     * If a worker attempts to perform an op requiring logging, this method should be called
     * and it should signal us that the assumption is broken
     * Need to be careful though, thread pools like to swallow and discard errors.
     */
    public void errIfWorkerWithoutTask(){
        if(workerThread && monitorOwnerRef == this) //we can log the events, but it may screw up the VC in replay
            throw new UnsupportedOperationException("Not handled yet");
    }

    @Override
    public void monitorenter(Object o) {
        errIfWorkerWithoutTask();
        super.monitorenter(o);
    }

    @Override
    public void wait(Object o, long timeout, int nanos) throws InterruptedException {
        errIfWorkerWithoutTask();
        super.wait(o, timeout, nanos);
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes) throws IOException {
        errIfWorkerWithoutTask();
        super.write(os, bytes);
    }

    @Override
    public void write(JMVXRandomAccessFile raf, int b) throws IOException {
        errIfWorkerWithoutTask();
        super.write(raf, b);
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes) throws IOException {
        errIfWorkerWithoutTask();
        super.write(raf, bytes);
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes, int off, int len) throws IOException {
        errIfWorkerWithoutTask();
        super.write(raf, bytes, off, len);
    }

    @Override
    public int write(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        errIfWorkerWithoutTask();
        return super.write(impl, fd, address, len);
    }

    @Override
    public int pwrite(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len, long position) throws IOException {
        errIfWorkerWithoutTask();
        return super.pwrite(impl, fd, address, len, position);
    }

    @Override
    public long writev(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        errIfWorkerWithoutTask();
        return super.writev(impl, fd, address, len);
    }

    @Override
    public void write(JMVXOutputStream os, int b) throws IOException {
        errIfWorkerWithoutTask();
        super.write(os, b);
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes, int off, int len) throws IOException {
        errIfWorkerWithoutTask();
        super.write(os, bytes, off, len);
    }

    @Override
    public int read(JMVXInputStream is) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(is);
    }

    @Override
    public int read(JMVXInputStream is, byte[] bytes) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(is, bytes);
    }

    @Override
    public int read(JMVXInputStream is, byte[] bytes, int off, int len) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(is, bytes, off, len);
    }

    @Override
    public int read(JMVXRandomAccessFile f) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(f);
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(f, bytes);
    }

    @Override
    public int read(JMVXFileChannel c, ByteBuffer dst) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(c, dst);
    }

    @Override
    public int read(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(impl, fd, address, len);
    }

    @Override
    public int pread(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len, long position) throws IOException {
        errIfWorkerWithoutTask();
        return super.pread(impl, fd, address, len, position);
    }

    @Override
    public long readv(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        errIfWorkerWithoutTask();
        return super.readv(impl, fd, address, len);
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes, int off, int len) throws IOException {
        errIfWorkerWithoutTask();
        return super.read(f, bytes, off, len);
    }

    public static class JobCreated implements Serializable {
        private static final long serialVersionUID = 1136336223052302205L;
        public final int id;
        public JobCreated(int id) {
            this.id = id;
        }
    }

    public static class TaskFinished implements Serializable {
        private static final long serialVersionUID = 5316884230995551165L;
        public final int stamp;

        public TaskFinished(int stamp) {
            this.stamp = stamp;
        }
    }
}
