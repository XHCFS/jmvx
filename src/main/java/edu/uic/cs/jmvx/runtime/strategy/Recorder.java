package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.Utils;
import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.*;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;
import edu.uic.cs.jmvx.runtime.util.recording.ReadRecorderUtility;
import edu.uic.cs.jmvx.vectorclock.VectorClock;
import org.apache.log4j.Logger;

import sun.misc.Signal;

import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;
import java.util.zip.CRC32;

public class Recorder extends Leader {

    //used to identify which jars come from JDK
    public static final String JAVA_HOME = System.getenv("JAVA_HOME");
    protected static AtomicInteger clockIndex = new AtomicInteger(VectorClock.START_INDEX);

    private Stack<String> classNames = new Stack();
    private Stack<ObjectOutputStream> classStreams = new Stack();
    private static ObjectOutputStream classes;
    private static ObjectOutputStream manifests;
    private static Set<String> manifestsRecorded = new HashSet<>();


    //private static ObjectOutputStream clinits;

    protected static final String recordingDir = JMVXRuntime.parameters.recordingDir;
    protected static final boolean validateWrites = JMVXRuntime.parameters.validateWrites;

    private ObjectOutputStream objects;
    protected ReadRecorderUtility readRecorder;
    protected static ConcurrentHashMap<String, Flushable> streamsToFlush = new ConcurrentHashMap<>();
    /*map of path to jar -> path of jar on replay. Logs where we expect to find code
    JDK classes: path -> path
    Program classes: path -> $REC/path
    All entries with $REC/path will be copied into the recordingDir at the end of the Recording, so they will be
    available for replay. Note the Replayer will resolve redirected pathnames as needed
     */
    private static ConcurrentHashMap<String, String> jarsManifest = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Long, String> openedZips = new ConcurrentHashMap<>();
    private static volatile boolean handlerInit = false;

    private static final int bufferSize = 10 * 1024 * 1024; //10 mb

    //shadow the fields we wish to overwrite
    protected static final boolean checksumCommunicationEnabled = false; //cannot use this optimization
    protected static final boolean readsFromDisk = false; //cannot use this optimization
    protected static final boolean circularBufferCommunicationEnabled = false; //cannot use this buffer here

    static {
        String role = JMVXRuntime.parameters.role;
        if(role != null && role.startsWith(Recorder.class.getSimpleName())) {
            initStaticStreams();
        }
    }

    protected static void initStaticStreams() {
        //want to record, safe to alter the filesystem
        //prevents us from truncating these files if Recorder is loaded by another class
        //make a fresh recording directory when this is called
        try {
            JMVXRuntime.enter();
            JMVXFile recDir = (JMVXFile) new File(recordingDir);
            if (recDir.$JMVX$exists()) {
                Files.walkFileTree(Paths.get(recordingDir), new FileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        JMVXFile f = (JMVXFile) file.toFile();
                        f.$JMVX$delete();
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        JMVXFile d = (JMVXFile) dir.toFile();
                        d.$JMVX$delete();
                        return FileVisitResult.CONTINUE;
                    }
                });
            }

            recDir.$JMVX$mkdirs();

            try {
                clinitOut = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(recordingDir, "recording.clinits.dat").toFile()), bufferSize));
                classes = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(recordingDir, "recording.classes.dat").toFile()), bufferSize));
                manifests = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(recordingDir, "recording.manifests.dat").toFile()), bufferSize));
            } catch (IOException e) {

            }
        } catch (IOException e){
            throw new RuntimeException("Failed to setup recording directory", e);
        } finally {
            JMVXRuntime.exit();
        }
    }

    public Recorder(){
        if(!handlerInit){
            handlerInit = true; //no one else shall run the following code
            Signal.handle(new Signal("USR2"), signal -> {
                flushData(false);
            });
        }
    }

    @Override
    public void main() {
        vectorClockIndex = clockIndex.getAndIncrement();
        vectorClockIndex = JMVXRuntime.clock.registerNewThread();
        main(vectorClockIndex, true);
    }

    /* doesn't alter the vector clock */
    public void main(int clockIndex, boolean makeStream){
        vectorClockIndex = clockIndex;
        try {
            this.objects = makeStream ? new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(recordingDir, "recording." + Thread.currentThread().getName() + ".dat").toFile()), bufferSize)) : null;
            this.toCoordinator = this.objects;
            if(makeStream) {
                this.toCoordinator.writeInt(vectorClockIndex);
                this.toCoordinator.flush();
                //need to initialize this where we can log it.
                //because it will be initialized when we add to streamsToFlush.
                //we miss the first one here, because JMVX is not initialized.... shit.
                //ThreadLocalRandom.current();
                synchronized (Recorder.class) {
                    streamsToFlush.put(Thread.currentThread().getName(), this.objects);
                }
            }
        } catch (IOException e) {
            throw new Error(e);
        }

        try {
            this.fromCoordinator = new ObjectInputStream(new InputStream() {
                @Override
                public int read() throws IOException {
                    throw new IOException("NOT SUPPORTED");
                }
            }) {
                @Override
                protected void readStreamHeader() throws IOException, StreamCorruptedException {
                    return;
                }
            };
        } catch (IOException e) {
            throw new Error(e);
        }

        super.log = Logger.getLogger(Recorder.class);
        readRecorder = new ReadRecorderUtility(avoidObjectSerialization, circularBufferCommunicationEnabled, false);
    }

    @Override
    protected void sendObjectToCoordinator(Serializable o) {
        try {
            objectCounter++;
            toCoordinator.writeObject(o);
            //toCoordinator.flush(); //removed to make better use of buffer
            toCoordinator.reset();
        } catch (IOException e) {
            throw new Error(e);
        }
    }


    /*Pass through for class loading*/
    @Override
    public boolean exists(JMVXFile f) {
        if(loading == 0) {
            return super.exists(f);
        }else{
            return f.$JMVX$exists();
        }
    }

    /*Pass through for class loading*/
    @Override
    public long lastModified(JMVXFile f) {
        if(loading == 0) {
            return super.lastModified(f);
        } else {
            return f.$JMVX$lastModified();
        }
    }

    @Override
    public boolean delete(JMVXFile f) {
        boolean ret = f.$JMVX$delete();
        sendObjectToCoordinator(new FileDelete(ret));
        objectCounter++;
        return ret;
    }

    @Override
    public void clinitStart(String cls) {
        /*
        Modified to enable record replay to go faster--the locking operations have been removed.
        Leader/Follower strategies needed the locking because the
        events were handled online--the Follower would need to know if an operation belong to a clinit so it could
        shuffle around events. When Replaying, all events exist on disk so we are free to grab them at will.

        This relies on an assumption, namely that threads will not be doing locking
        operations in a clinit, so the vector clock will not be corrupted.
        An assertion has been added to monitorenter to crash the program if ever that
        happens. If the assumption holds, we can skip a lot of locking.
         */
        inClinit++;

        /**
         * Zip files are weird.
         * We may log zip file ops in a clinit or class load, even if we don't care
         * about that class/clinit. E.g., loading a java.* class
         * We don't need to log that, but the zip operations always use the strategy
         * so we can check if they are safe. But we may not need to log anything...
         * (this was the intent of the care stack...)
         */

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        JMVXRuntime.enter();
        try {
            clinitsByteStreams.push(bos);
            clinitsStreams.push(toCoordinator);
            try {
                toCoordinator = new ObjectOutputStream(bos);
                //TODO: check, should this be vector clock index?
                toCoordinator.writeObject(null); // This is needed to avoid an "IOException stream active" when calling ObjectOutputStream.reset()
            } catch (IOException e) {
                throw new Error(e);
            }
        } finally {
            JMVXRuntime.exit();
        }
    }

    @Override
    public void clinitEnd(String cls) {
        JMVXRuntime.enter();
        try {
            try {
                this.toCoordinator.close();
                byte[] buf = clinitsByteStreams.pop().toByteArray();
                synchronized (clinitOut) {
                    clinitOut.writeObject(cls);
                    clinitOut.writeObject(buf);
                    clinitOut.flush();
                    clinitOut.reset();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            JMVXRuntime.exit();
        }
        toCoordinator = clinitsStreams.pop();
        inClinit--;
    }

    @Override
    public void exitLogic() {
        if (!Thread.currentThread().getName().equals(JMVXRuntime.mainThread.getName()))
            return;

        flushData(true);
    }

    /**
     * Flushes streams and copies jar files
     */
    protected static void flushData(boolean closeFiles) {
        for (Flushable f : streamsToFlush.values()) {
            try {
                f.flush();
            } catch (IOException e) {
                continue;
            }
        }

        try (ObjectOutputStream jarPaths = new ObjectOutputStream(new BufferedOutputStream(
                new FileOutputStream(Paths.get(recordingDir, "jarPaths.dat").toString())))){
            jarPaths.writeObject(jarsManifest);
        } catch (IOException e) {
            throw new Error("Failed to create jarPaths.dat", e);
        }

        /*convert marked jars, e.g., "$REC/path/to/jarname.jar to $REC/jarname.jar
          we do this so the recording directory can be changed between record and replay
          A replay can be moved to a new directory without breaking it*/

        jarsManifest.forEach((rawPath, alteredPath) -> {
            if(alteredPath.startsWith("$REC")) { //a jar that we want to copy, not included in jvm
                Path jarPath = Paths.get(rawPath);
                Path recJar = Paths.get(recordingDir).resolve(Paths.get(alteredPath).getFileName());
                try {
                    Files.copy(jarPath, recJar, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new Error("Failed to copy " + jarPath + " to " + recJar, e);
                }
            }
        });

        /*
        Terminates the streams.
        Added for sanity.
        Checking the behavior of RRWriter is done via the Copier class, which copies a recording
        Leaving the closure of these streams to the JVM causes them to exclude a terminating byte,
        which Copier adds (because it handles the streams more gracefully).

        Not closing these files won't cause an error,
        in fact, leaving them open may help us deal with code in shutdown hooks.
        Not closing them does make verifying the RRWriter/Copier more annoying.
         */
        if(closeFiles) {
            try {
                classes.close();
                clinitOut.close();
            } catch (IOException _) {
            }
        }
    }

    protected int loading = 0;

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name) throws ClassNotFoundException {
        return this.loadClass(loader, name, true);
    }

    private final static ArrayList<String> dontLogLoad = new ArrayList<>();

    public static boolean dontLogLoad(String name){
        return dontLogLoad.stream().anyMatch((denied) -> name.startsWith(denied));
    }

    static {
        dontLogLoad.add("edu.uic.cs.jmvx.");
        dontLogLoad.add("java.");
        dontLogLoad.add("sun.");
        dontLogLoad.add("com.sun.");
        dontLogLoad.add("javax.");
    }

    // Cache of loaded classes per each classloader, or exceptions if the class is not found
    // This cache is not needed on the replayer as the classloaders themselves automatically cache results without calling any instrumented methods
    private static ConcurrentHashMap<JMVXClassLoader, ConcurrentHashMap<String, Object>> cache = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Object> empty = new ConcurrentHashMap<>();

    private static final JMVXClassLoader extLoader = (JMVXClassLoader) ClassLoader.getSystemClassLoader().getParent();

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name, boolean resolve) throws ClassNotFoundException {
        /*calling a class loader specifically can lead to issues. For instance in Xalan with DacapoClassLoader
        Xalan has an ObjectFactory that manually calls loadClass with a DacapoClassLoaderObject
        DacapoClassLoader.loadClass is synchronized--unlike most others
        The call chain creates a deadlock
        loadClass -> Recorder loadClass -> Dacapo loadClass (blocks)
        Another thread run at the same time can grab the DacapoClassLoader lock and lead to deadlock
        loadClass -> Recorder (blocks)
        */
        synchronized (loader) {

                Object cached = cache.getOrDefault(loader, empty).get(name);
                if (cached != null) {
                    if (cached instanceof Class<?>)
                        return (Class<?>)cached;
                    else
                        throw (ClassNotFoundException) cached;
                }

                this.loading++;
                classNames.push(name);

                try {
                    if(name.startsWith("edu.uic.cs.jmvx") && ((ClassLoader)loader).getParent() != null){
                        //skip JMVX Classes
                        try{
                            JMVXRuntime.enter();
                            return extLoader.$JMVX$loadClass(name, resolve);
                        }finally{
                            JMVXRuntime.exit();
                        }
                    }

                    JMVXRuntime.enter();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    //bos should still be in stack frame and have a correct pointer

                    try {
                        classStreams.push(toCoordinator); //need to preserve this
                        toCoordinator = new ObjectOutputStream(bos);
                        toCoordinator.writeObject(null); // This is needed to avoid an "IOException stream active" when calling ObjectOutputStream.reset()
                    } catch (IOException e) {
                        throw new Error(e);
                    } finally {
                        JMVXRuntime.exit();
                    }

                    Class<?> ret = null;
                    ConcurrentHashMap<String, Object> m = getClassLoaderCache(loader);
                    try {
                        ret = loader.$JMVX$loadClass(name, resolve);
                        m.put(name, ret);
                        return ret;
                    } catch (ClassNotFoundException e) {
                        m.put(name, e);
                        throw e;
                    } finally {

                        JMVXRuntime.enter();
                        try {
                            synchronized (Recorder.class) {
                                toCoordinator.close();
                                toCoordinator = classStreams.pop();
                                //classNames.pop(); //done loading
                                //assert s.equals(name); //if this is false I've messed up somewhere
                                this.classes.writeObject(loader.getClass().getName());
                                this.classes.writeObject(name);
                                this.classes.writeObject(bos.toByteArray());
                                this.classes.flush();
                                this.classes.reset();
                            }
                        } catch (IOException e) {
                            throw new Error(e);
                        } finally {
                            JMVXRuntime.exit();
                        }

                    }
                } finally {
                    classNames.pop(); //done loading
                    loading--;
                    //this.loading = !classNames.empty();//false;
                    //super.toCoordinator = this.objects;
                }
        }
    }

    private ConcurrentHashMap<String, Object> getClassLoaderCache(JMVXClassLoader loader) {
        synchronized (cache) {
            ConcurrentHashMap<String, Object> m = cache.get(loader);
            if (m == null) {
                m = new ConcurrentHashMap<>();
                cache.put(loader, m);
            }
            return m;
        }
    }

    /* STOP GAP SOLUTION
     * LeaderRecorder optimizes use of RandomAccess files, but the regular Leader does not
     * Override them here until the optimizations are back ported
     */

    @Override
    public int read(JMVXRandomAccessFile f) throws IOException {
        try {
            int ret = f.$JMVX$read();
            readRecorder.recordReadByte(toCoordinator, buffer, OptimizedEvent.RANDOM_ACCESS_READ, ret);
            return ret;
        } catch (IOException e) {
            readRecorder.logException(toCoordinator, e);
            throw e;
        }
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes) throws IOException {
        try {
            int ret = f.$JMVX$read(bytes);
            readRecorder.recordRead(toCoordinator, buffer, OptimizedEvent.RANDOM_ACCESS_READ, bytes, 0, bytes.length, ret);
            return ret;
        } catch (IOException e) {
            readRecorder.logException(toCoordinator, e);
            throw e;
        }
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes, int off, int len) throws IOException {
        try {
            int ret = f.$JMVX$read(bytes, off, len);
            readRecorder.recordRead(toCoordinator, buffer, OptimizedEvent.RANDOM_ACCESS_READ,
                    bytes, off, len, ret);
            return ret;
        } catch (IOException e) {
            readRecorder.logException(toCoordinator, e);
            throw e;
        }
    }

    @Override
    public void open(JMVXRandomAccessFile raf, String name, int mode) throws FileNotFoundException {
        try {
            raf.$JMVX$open(name, mode);
            this.sendObjectToCoordinator(new Leader.RAOpen(name, mode));
        } catch (FileNotFoundException e) {
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    /** Writes a sigil OptimizedEvent.EXCEPTION tag to indicate an exception is coming
     * if avoidObjectSerialization is on. Otherwise, it just logs an exception
     * This method should only be used by events that have an optimized representation
     * eg, those which have special logic for avoidObjectSerialization */
    private void logExceptionPossiblyWithSigil(IOException e) {
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            //placeholder to say throw an IOException...
            //buffer.writeData(new byte[0], -1, 0, true);
            try {
                OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, toCoordinator);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to write to log file", ex);
            }
        }
        sendObjectToCoordinator(e);
    }

    private void logRandomAccessWriteByte(int b) {
        try {
            if(avoidObjectSerialization) {
                if (validateWrites) {
                    OptimizedEvent.writeTag(OptimizedEvent.RANDOM_ACCESS_WRITE, this.toCoordinator);
                    this.toCoordinator.writeInt(Integer.BYTES); //b
                    this.toCoordinator.writeInt(b);
                } else {
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, this.toCoordinator);
                }
            }else{
                sendObjectToCoordinator(new Leader.RAWrite(b));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, int b) throws IOException {
        try {
            raf.$JMVX$write(b);
            logRandomAccessWriteByte(b);
        } catch (IOException e) {
            logExceptionPossiblyWithSigil(e);
            throw e;
        }
    }

    private CRC32 crc32 = new CRC32();

    private void logRandomAccessWriteByteArray(byte[] bytes) {
        byte[] bytesToSend = checksumCommunicationEnabled ? Utils.getChecksum(bytes, crc32) : bytes;
        try {
            if (avoidObjectSerialization) {
                if (validateWrites) {
                    OptimizedEvent.writeTag(OptimizedEvent.RANDOM_ACCESS_WRITE, this.toCoordinator);
                    this.toCoordinator.writeInt(bytes.length); //b
                    this.toCoordinator.write(bytes, 0, bytes.length);
                } else {
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, this.toCoordinator);
                }
            } else {
                sendObjectToCoordinator(new Leader.RAWriteB(bytesToSend));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes) throws IOException {
        try {
            raf.$JMVX$write(bytes);
            logRandomAccessWriteByteArray(bytes);
        } catch (IOException e) {
            logExceptionPossiblyWithSigil(e);
            throw e;
        }
    }

    private void logRandomAccessWriteWithOffsetAndLength(byte[] bytes, int off, int len) {
        byte[] bytesToSend = checksumCommunicationEnabled ? Utils.getChecksum(bytes, off, len, crc32) : bytes;
        try {
            if (avoidObjectSerialization) {
                    if(validateWrites) {
                        OptimizedEvent.writeTag(OptimizedEvent.RANDOM_ACCESS_WRITE, this.toCoordinator);
                        this.toCoordinator.writeInt(
                                (Integer.BYTES * 2) //offset, len
                                        + len //space for bytes
                        );
                        this.toCoordinator.writeInt(off);
                        this.toCoordinator.writeInt(len);
                        this.toCoordinator.write(bytes, off, len);
                    }else{
                        OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, this.toCoordinator);
                    }
            } else {
                sendObjectToCoordinator(new Leader.RAWriteBII(bytesToSend, off, len));
            }
        } catch(IOException e){
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes, int off, int len) throws IOException {
        try {
            raf.$JMVX$write(bytes, off, len);
            logRandomAccessWriteWithOffsetAndLength(bytes, off, len);
        } catch (IOException e) {
            logExceptionPossiblyWithSigil(e);
            throw e;
        }
    }

    @Override
    public void seek(JMVXRandomAccessFile f, long pos) throws IOException {
        try {
            f.$JMVX$seek(pos);
            if (avoidObjectSerialization) {
                /*byte[] data = new byte[] {
                        (byte)  pos,
                        (byte) (pos >> 8),
                        (byte) (pos >> 16),
                        (byte) (pos >> 24),
                        (byte) (pos >> 32),
                        (byte) (pos >> 40),
                        (byte) (pos >> 48),
                        (byte) (pos >> 56)};
                buffer.writeWriteOpData(data, 0, 8, true);*/
                //skipped when optimized
                //TODO ACTUALLY WRITE THIS EVENT
            } else {
                try{
                    f.$JMVX$seek(pos);
                    sendObjectToCoordinator(new Leader.RASeek(pos));
                }catch (IOException e){
                    sendObjectToCoordinator(e);
                }
            }
        } catch (IOException e) {
            //TODO HANDLE EXCEPTION
            throw new Error("Not supported");
        }
    }

    protected long[] clockCopy = new long[0];
    private byte[] bytes = new byte[0];

    private void writeOptimizedClock(Object o, OptimizedEvent fastTag, OptimizedEvent slowTag) {
        /* The clock needs to be incremented no matter what
        * The replayer needs to know how long to wait while on the slow path
        * Incrementing on the fast path performs this bookkeeping
        * The increments logs "how many [fast] operations do other threads need to wait for"
        * Without the increment, the replayer may preempt fast path ops with slow path ones
        * which should take place later */
        VectorClock clock = JMVXRuntime.clock;
        long[] newClockCopy = clock.increment(vectorClockIndex, clockCopy);
        if(canTakeClockFastPath(o)){
            writeClockFastPath(fastTag);
        }else{
            writeClockSlowPath(slowTag, newClockCopy);
        }
    }

    @Override
    protected void writeClockFastPath(OptimizedEvent fastTag) {
        if(avoidObjectSerialization) {
            //fast write
            try {
                OptimizedEvent.writeTag(fastTag, this.toCoordinator);
            } catch (IOException e) {
                throw new Error(e);
            }
        }else{
            sendObjectToCoordinator(serializeClockEventFromTag(fastTag, null));
        }
    }

    @Override
    protected void writeClockSlowPath(OptimizedEvent slowTag, long[] newClockCopy) {
        if(avoidObjectSerialization) {
            if (newClockCopy != clockCopy) {
                clockCopy = newClockCopy;
                bytes = new byte[clockCopy.length * 8];
            }

        /* endianness of the system matters!
        If you move a recording from a big to a little endian system the replayer may have issues */
            JMVXRuntime.unsafe.copyMemory(clockCopy, CircularBuffer.longOffset, bytes, CircularBuffer.byteOffset, bytes.length);

            try {
                OptimizedEvent.writeTag(slowTag, this.toCoordinator);
            /*
            Size of the clock is inferred based on this
            Less expensive to compute the size than perform more IO
             */
                this.toCoordinator.writeInt(
                        //Integer.BYTES //hashcode
                        +bytes.length //clock itself
                );
                //this.toCoordinator.writeInt(id);
                this.toCoordinator.write(bytes);
            } catch (IOException e) {
                throw new Error(e);
            }
        }else{
            sendObjectToCoordinator(serializeClockEventFromTag(slowTag, newClockCopy));
        }
    }
    
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
        *  circular buffer fine. */
        if(canTakeClockFastPath(o)){
            clock.increment(vectorClockIndex, null); //skip the array copy
            writeClockFastPath(OptimizedEvent.FAST_MONITOR_ENTER);
        }else{
            long[] newClockCopy = clock.increment(vectorClockIndex, clockCopy);
            writeClockSlowPath(OptimizedEvent.MONITOR_ENTER, newClockCopy);
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
        /* The clock needs to be incremented no matter what
        * The replayer needs to know how long to wait while on the slow path
        * Incrementing on the fast path performs this bookkeeping
        * The increments logs "how many [fast] operations do other threads need to wait for"
        * Without the increment, the replayer may preempt fast path ops with slow path ones
        * which should take place later */
        VectorClock clock = JMVXRuntime.clock;
        if(canTakeClockFastPath(o)){
            clock.increment(vectorClockIndex, null); //skip the array copy
            writeClockFastPath(OptimizedEvent.FAST_WAIT);
        }else{
            long[] newClockCopy = clock.increment(vectorClockIndex, clockCopy);
            writeClockSlowPath(OptimizedEvent.WAIT, newClockCopy);
        }
    }

    /*Didn't really help for PMD with FLUSH_AFTER = 10_000, 100_000, 250_000, or 500_000

    private final int FLUSH_AFTER_MONITORS = Integer.parseInt(System.getProperty("flushAfter", "10000"));
    private int monitorsSinceFlush = 0;

    @Override
    public void monitorexit(Object o) {
        super.monitorexit(o);
        monitorsSinceFlush++;
        //make sure we don't hold the lock. Avoid IO in critical sections.
        if(monitorsSinceFlush >= FLUSH_AFTER_MONITORS && !Thread.holdsLock(o)){
            try {
                toCoordinator.flush();
            }catch (IOException _){ }
            monitorsSinceFlush = 0;
        }
    }*/

    @Override
    public long currentTimeMillis() {
        if (avoidObjectSerialization) {
            long ret = System.currentTimeMillis();
            try {
                OptimizedEvent.writeTag(OptimizedEvent.CURRENT_TIME, this.toCoordinator);
                this.toCoordinator.writeInt(Long.BYTES);
                this.toCoordinator.writeLong(ret);
            } catch (IOException e) {
                throw new Error(e);
            }
            return ret;
        } else {
            return super.currentTimeMillis();
        }
    }

    @Override
    public int read(JMVXInputStream is) throws IOException {
        try {
            int ret = is.$JMVX$read();
            /*NOTE: the semantics here log a ReadBII, so the return value has a different meaning!
             * */
            readRecorder.recordReadByte(toCoordinator, buffer, OptimizedEvent.READ, ret);
            debug(is, ret);
            return ret;
        } catch (IOException e) {
            readRecorder.logException(toCoordinator, e);
            throw e;
        }
    }

    @Override
    public int read(JMVXInputStream is, byte[] bytes) throws IOException {
        try{
            int ret = is.$JMVX$read(bytes);
            readRecorder.recordRead(toCoordinator, buffer, OptimizedEvent.READ,
                    bytes, 0, bytes.length, ret);
            debug(is, ret, bytes.length);
            return ret;
        } catch (IOException e) {
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int read(JMVXInputStream is, byte[] bytes, int off, int len) throws IOException {
        /* Passthrough is for loading from .jar files.
        .class files are not copied into the recording directory currenty, so we have to log them
        We can implement a similar mechanism to zipfiles open/getEntry to save .class files used
        during loading.
        Need to overwrite open(JMVXFileInputStream ...), log opened .class files and check if
        we read them (here) during loading. Or we can modify the instrumentation of
        JMVXFileInputStream to add a getter for the path which is the source of the stream
        and check that here. That will tell us which file to copy into the recording directory.

        NOTE: the reads from .class files ARE ALWAYS SERIALIZED
        reads when not loading may be optimized based on avoidObjectSerialization setting
        This is a hotfix mimicking the logic in the hybrid mvx/recorder so that the replayer works with
        both this recorder and the hybrid*/
        if (loading > 0 && is.getClass().equals(JMVXRuntime.zipFileInputStreamClass)) {
            return is.$JMVX$read(bytes, off, len);
        } else {
            try {
                int ret = is.$JMVX$read(bytes, off, len);
                readRecorder.recordRead(toCoordinator, buffer, OptimizedEvent.READ,
                        bytes, off, len, ret);
                return ret;
            } catch (IOException e) {
                //need the sentinel
                readRecorder.logException(toCoordinator, e);
                throw e;
            }
        }
    }

    @Override
    public void write(JMVXOutputStream os, int b) throws IOException {
        if (avoidObjectSerialization) {
            try {
                os.$JMVX$write(b);
                if(validateWrites) {
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE, this.toCoordinator);
                    this.toCoordinator.writeInt(Integer.BYTES); //b
                    this.toCoordinator.writeInt(b);
                }else{
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, this.toCoordinator);
                }
            } catch(IOException e){
                //placeholder to signal to look for an exception
                OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, this.toCoordinator);
                sendObjectToCoordinator(e);
                throw e;
            }
        } else {
            super.write(os, b);
        }
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes) throws IOException {
        if (avoidObjectSerialization) {
            try {
                os.$JMVX$write(bytes);
                if(validateWrites) {
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE, this.toCoordinator);
                    this.toCoordinator.writeInt(bytes.length); //b
                    this.toCoordinator.write(bytes, 0, bytes.length);
                }else{
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, this.toCoordinator);
                }
            } catch(IOException e){
                //placeholder to signal to look for an exception
                OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, this.toCoordinator);
                sendObjectToCoordinator(e);
                throw e;
            }
        } else {
            super.write(os, bytes);
        }
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes, int off, int len) throws IOException {
        if (avoidObjectSerialization) {
            try {
                os.$JMVX$write(bytes, off, len);
                if(validateWrites) {
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE, this.toCoordinator);
                    this.toCoordinator.writeInt(
                            (Integer.BYTES * 2) //offset, len
                                    + len //space for bytes
                    );
                    this.toCoordinator.writeInt(off);
                    this.toCoordinator.writeInt(len);
                    this.toCoordinator.write(bytes, off, len);
                }else{
                    OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, this.toCoordinator);
                }
            } catch(IOException e){
                OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, this.toCoordinator);
                sendObjectToCoordinator(e);
                throw e;
            }
        } else {
            super.write(os, bytes, off, len);
        }
    }


    @Override
    public int available(JMVXInputStream is) throws IOException {
        if (avoidObjectSerialization) {
            boolean errSrcIsJMVX = false;
            /*
            Avoids conflicting the error handler
            If we merge the available call and the logging code,
            we cannot tell if an IOException is from is.available or JMVX
             */
            try {
                int ret = is.$JMVX$available();
                errSrcIsJMVX = true;
                OptimizedEvent.writeTag(OptimizedEvent.AVAILABLE, this.toCoordinator);
                this.toCoordinator.writeInt(Integer.BYTES); //ret, should we make this inferred for some ops?
                this.toCoordinator.writeInt(ret);
                return ret;
            }catch (IOException e) {
                if (errSrcIsJMVX) {
                    throw new Error("available failed to log result", e);
                }else{ //log error and then throw it
                    try{
                        OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, this.toCoordinator);
                        sendObjectToCoordinator(e);
                        throw e;
                    }catch (IOException ee){
                        throw new Error("Failed to log result", ee);
                    }
                }
            }
        } else {
            return super.available(is);
        }
    }

    /*
    Socket related methods have logic to forward file descriptors
    Which cannot be done in RR.
    To make up for this, we override those methods and remove some code
     */
    @Override
    public void connect(JMVXSocket sock, SocketAddress endpoint, int timeout) throws IOException {
        //Leader connects normally
        try {
            sock.$JMVX$connect(endpoint, timeout);
            //may need to refactor getFD to prevent exceptions from needing to be added...
            sendObjectToCoordinator(new Connect(endpoint, timeout));
        } catch (IOException e) {
            //TODO catch other exceptions thrown by connect
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void bind(JMVXServerSocket serv, SocketAddress endpoint, int backlog) throws IOException {
        try {
            //bind to the endpoint
            serv.$JMVX$bind(endpoint, backlog);
            sendObjectToCoordinator(new Bind(endpoint, backlog));
        }catch(IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public Socket accept(JMVXServerSocket serv) throws IOException {
        try {
            //get the new client socket
            sendObjectToCoordinator(new AcceptBegin());
            Socket sock = serv.$JMVX$accept();
            sendObjectToCoordinator(new AcceptEnd());
            return sock;
        } catch (IOException e) {
            //TODO catch other exceptions thrown by connect
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    public static boolean loggableJar(String jarName){
        return !(jarName.startsWith(JAVA_HOME) || jarName.contains("JavaMVX"));
    }

    @Override
    public void initIDs() {
        JMVXRuntime.zipfile.$JMVX$initIDs();
    }

    @Override
    public long getEntry(long jzfile, byte[] name, boolean addSlash) {
        long ret = JMVXRuntime.zipfile.$JMVX$getEntry(jzfile, name, addSlash);
        String rawPath = openedZips.get(jzfile);
        if(loading == 0){
            if(rawPath != null)
                jarsManifest.putIfAbsent(rawPath, rawPath);
            sendObjectToCoordinator(new Recorder.GetEntry(jzfile, name, addSlash, -ret));
        }else{ //got an entry during loading, prep it for logging
            if(rawPath != null){
                if(loggableJar(rawPath)) //Program jar, copied to recordingDir later in flushData
                    jarsManifest.put(rawPath, Paths.get("$REC", rawPath).toString()); //$REC/path marks this for copying
                else //JDK jar, path is valid unchanged
                    jarsManifest.put(rawPath, rawPath);
            } //if rawPath is null, then the Jar was opened before JMVX was init'ed and will be okay during replay
            //sendObjectToCoordinator(new Recorder.GetEntry(jzfile, name, addSlash, ret));
        }
        return ret;
    }

    @Override
    public void freeEntry(long jzfile, long jzentry) {
        JMVXRuntime.zipfile.$JMVX$freeEntry(jzfile, jzentry);
    }

    @Override
    public long getNextEntry(long jzfile, int i) {
        long ret = JMVXRuntime.zipfile.$JMVX$getNextEntry(jzfile, i);
        if(loading == 0){
            sendObjectToCoordinator(new Recorder.GetNextEntry(jzfile, i, ret));
        }
        return ret;
    }

    @Override
    public void close(long jzfile) {
        JMVXRuntime.zipfile.$JMVX$close(jzfile);
    }

    @Override
    public long open(String name, int mode, long lastModified, boolean usemmap) throws IOException {
        long ret = -1L;
        if(loading > 0){ //use the jar normally for loading
            try{
                ret = JMVXRuntime.zipfile.$JMVX$open(name, mode, lastModified, usemmap);
            }catch(IOException e){
                throw e;
            }
        }else{ //not loading, need to make an event
            try{
                ret = JMVXRuntime.zipfile.$JMVX$open(name, mode, lastModified, usemmap);
                //-ret is used to taint the pointer! Now we can easily track pointers from logfiles in the Replayer
                sendObjectToCoordinator(new Open(name, mode, lastModified, usemmap, -ret));
            }catch(IOException e){
                sendObjectToCoordinator(e);
                throw e;
            }
        }
        /*This could be a jar. We don't know if we will need to copy this to the recording yet.
        Save info the reference the path later when we can tell (getEntry).
         */
        openedZips.put(ret, name);
        return ret;
    }

    @Override
    public int getTotal(long jzfile) {
        int ret = JMVXRuntime.zipfile.$JMVX$getTotal(jzfile);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetTotal(jzfile, ret));
        return ret;
    }

    @Override
    public boolean startsWithLOC(long jzfile) {
        boolean ret = JMVXRuntime.zipfile.$JMVX$startsWithLOC(jzfile);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.StartsWithLOC(jzfile, ret));
        return ret;
    }

    @Override
    public int read(long jzfile, long jzentry, long pos, byte[] b, int off, int len) {
        int ret = JMVXRuntime.zipfile.$JMVX$read(jzfile, jzentry, pos, b, off, len);
        byte[] dat = new byte[ret];
        System.arraycopy(b, off, dat, 0, ret);
        //handled by Streams
        //if(loading == 0)
            //sendObjectToCoordinator(dat);//new ZipRead(jzfile, jzentry, pos, b, off, len, ret));
        //will be wrapped by a call to a ZipFileInputStream
        return ret;
    }

    @Override
    public long getEntryTime(long jzentry) {
        long ret = JMVXRuntime.zipfile.$JMVX$getEntryTime(jzentry);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntryTime(jzentry, ret));
        return ret;
    }

    @Override
    public long getEntryCrc(long jzentry) {
        long ret = JMVXRuntime.zipfile.$JMVX$getEntryCrc(jzentry);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntryCrc(jzentry, ret));
        return ret;
    }

    @Override
    public long getEntryCSize(long jzentry) {
        long ret = JMVXRuntime.zipfile.$JMVX$getEntryCSize(jzentry);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntryCSize(jzentry, ret));
        return ret;
    }

    @Override
    public long getEntrySize(long jzentry) {
        long ret = JMVXRuntime.zipfile.$JMVX$getEntrySize(jzentry);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntrySize(jzentry, ret));
        return ret;
    }

    @Override
    public int getEntryMethod(long jzentry) {
        int ret = JMVXRuntime.zipfile.$JMVX$getEntryMethod(jzentry);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntryMethod(jzentry, ret));
        return ret;
    }

    @Override
    public int getEntryFlag(long jzentry) {
        int ret = JMVXRuntime.zipfile.$JMVX$getEntryFlag(jzentry);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntryFlag(jzentry, ret));
        return ret;
    }

    @Override
    public byte[] getCommentBytes(long jzfile) {
        byte[] ret = JMVXRuntime.zipfile.$JMVX$getCommentBytes(jzfile);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetCommentBytes(jzfile, ret));
        return ret;
    }

    @Override
    public byte[] getEntryBytes(long jzentry, int type) {
        byte[] ret = JMVXRuntime.zipfile.$JMVX$getEntryBytes(jzentry, type);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetEntryBytes(jzentry, type, ret));
        return ret;
    }

    @Override
    public String getZipMessage(long jzfile) {
        String ret = JMVXRuntime.zipfile.$JMVX$getZipMessage(jzfile);
        if(loading == 0)
            sendObjectToCoordinator(new Recorder.GetZipMessage(jzfile, ret));
        return ret;
    }

    @Override
    public String[] getMetaInfEntryNames(JMVXJarFile jarfile) {
        /*
        When we load a class from a jar file for the first time, we may read manifest
        (or what in dacapo is usually a manifest list)
        We store these in a separate file,to be parsed at the start of replay
        because we are not guaranteed that the first class loaded (for a given jar)
        will be the same.
        If we logged this with the load class data, we may miss it during the replay and diverge!
        By storing it separate, we can play around this ordering issue.
         */
        JarFile jf = (JarFile)jarfile;
        if(loading > 0){
            return jarfile.$JMVX$getMetaInfEntryNames();
        }
        synchronized (manifests) {
            String[] ret = jarfile.$JMVX$getMetaInfEntryNames();
            String jarName = jf.getName();
            if(!manifestsRecorded.contains(jarName)){
                try {
                    manifests.writeObject(jarName);
                    manifests.writeObject(ret);
                    manifests.flush();
                    manifests.reset();
                }catch (IOException e){}
                manifestsRecorded.add(jarName);
            }
            //sendObjectToCoordinator(new Recorder.GetMetaInfEntryNames(ret));
            return ret;
        }
    }

    public static class GetEntry implements Serializable {
        private static final long serialVersionUID = 7839962330668504474L;
        public long ret;
        public long jzfile;
        public byte[] name;
        public boolean addSlash;
        public GetEntry(long jzfile, byte[] name, boolean addSlash, long ret) {
            this.ret = ret;
            this.jzfile = jzfile;
            this.name = name;
            this.addSlash = addSlash;
        }
    }

    public static class FreeEntry implements Serializable {
        private static final long serialVersionUID = -7808813808465508112L;
        public long jzfile;
        public long jzentry;
        public FreeEntry(long jzfile, long jzentry) {
            this.jzentry = jzfile;
            this.jzentry = jzentry;
        }
    }

    public static class GetNextEntry implements Serializable {
        private static final long serialVersionUID = 7120384860841771824L;
        public long jzfile;
        public int i;
        public long ret;

        public GetNextEntry(long jzfile, int i, long ret) {
            this.jzfile = jzfile;
            this.i = i;
            this.ret = ret;
        }
    }

    public static class Close implements Serializable {
        private static final long serialVersionUID = 6278188691004185593L;
        public long jzfile;

        public Close(long jzfile) {
            this.jzfile = jzfile;
        }
    }

    public static class Open implements Serializable {
        private static final long serialVersionUID = -4659045869787516973L;
        public String name;
        public int mode;
        public long lastModified;
        public boolean usemmap;
        public long ret;

        public Open(String name, int mode, long lastModified, boolean usemmap, long ret) {
            this.name = name;
            this.mode = mode;
            this.lastModified = lastModified;
            this.usemmap = usemmap;
            this.ret = ret;
        }
    }

    public static class GetTotal implements Serializable {
        private static final long serialVersionUID = 8151887101361260707L;
        public long jzfile;
        public int ret;

        public GetTotal(long jzfile, int ret) {
            this.jzfile = jzfile;
            this.ret = ret;
        }
    }

    public static class StartsWithLOC implements Serializable {
        private static final long serialVersionUID = 4858444449169345403L;
        public long jzfile;
        public boolean ret;

        public StartsWithLOC(long jzfile, boolean ret) {
            this.jzfile = jzfile;
            this.ret = ret;
        }
    }

    public static class ZipRead implements Serializable {
        private static final long serialVersionUID = -1629833004100983667L;
        public long jzfile;
        public long jzentry;
        public long pos;
        public byte[] b;
        public int off;
        public int len;
        public int ret;

        public ZipRead(long jzfile, long jzentry, long pos, byte[] b, int off, int len, int ret) {
            this.jzfile = jzfile;
            this.jzentry = jzentry;
            this.pos = pos;
            this.b = b;
            this.off = off;
            this.len = len;
            this.ret = ret;
        }
    }

    public static class GetEntryTime implements Serializable {
        private static final long serialVersionUID = -7998478397528888949L;
        public long jzentry;
        public long ret;

        public GetEntryTime(long jzentry, long ret) {
            this.jzentry = jzentry;
            this.ret = ret;
        }
    }

    public static class GetEntryCrc implements Serializable {
        private static final long serialVersionUID = 7642058854053133814L;
        public long jzentry;
        public long ret;

        public GetEntryCrc(long jzentry, long ret) {
            this.jzentry = jzentry;
            this.ret = ret;
        }
    }

    public static class GetEntryCSize implements Serializable {
        private static final long serialVersionUID = 1623618058666419499L;
        public long jzentry;
        public long ret;

        public GetEntryCSize(long jzentry, long ret) {
            this.jzentry = jzentry;
            this.ret = ret;
        }
    }

    public static class GetEntrySize implements Serializable {
        private static final long serialVersionUID = -1673714960141495208L;
        public long jzentry;
        public long ret;

        public GetEntrySize(long jzentry, long ret) {
            this.jzentry = jzentry;
            this.ret = ret;
        }
    }

    public static class GetEntryMethod implements Serializable {
        private static final long serialVersionUID = 5407894925840783656L;
        public long jzentry;
        public int ret;

        public GetEntryMethod(long jzentry, int ret) {
            this.jzentry = jzentry;
            this.ret = ret;
        }
    }

    public static class GetEntryFlag implements Serializable {
        private static final long serialVersionUID = -5967129126348540454L;
        public long jzentry;
        public int ret;

        public GetEntryFlag(long jzentry, int ret) {
            this.jzentry = jzentry;
            this.ret = ret;
        }
    }

    public static class GetCommentBytes implements Serializable {
        private static final long serialVersionUID = -557434812706667343L;
        public long jzfile;
        public byte[] ret;

        public GetCommentBytes(long jzfile, byte[] ret) {
            this.jzfile = jzfile;
            this.ret = ret;
        }
    }

    public static class GetEntryBytes implements Serializable {
        private static final long serialVersionUID = 3073033508482597328L;
        public long jzentry;
        public int type;
        public byte[] ret;

        public GetEntryBytes(long jzentry, int type, byte[] ret) {
            this.jzentry = jzentry;
            this.type = type;
            this.ret = ret;
        }
    }

    public static class GetZipMessage implements Serializable {
        private static final long serialVersionUID = -8967876306364293268L;
        public long jzfile;
        public String ret;

        public GetZipMessage(long jzfile, String ret) {
            this.jzfile = jzfile;
            this.ret = ret;
        }
    }

    public static class GetMetaInfEntryNames implements Serializable {
        private static final long serialVersionUID = -3620585514632163682L;
        public final String[] ret;
        public GetMetaInfEntryNames(String[] ret) {
            this.ret = ret;
        }
    }
}
