package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.Utils;
import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.coordinate.Coordinator;
import edu.uic.cs.jmvx.coordinate.CoordinatorThread;
import edu.uic.cs.jmvx.runtime.*;
import edu.uic.cs.jmvx.runtime.pools.*;
import edu.uic.cs.jmvx.runtime.util.*;
import edu.uic.cs.jmvx.runtime.util.recording.ReadRecorderUtility;
import edu.uic.cs.jmvx.vectorclock.VectorClock;
import org.apache.log4j.Logger;
import org.newsclub.net.unix.AFUNIXSocket;
import sun.misc.Unsafe;
import sun.nio.fs.UnixException;
import sun.nio.fs.UnixFileAttributes;
import sun.nio.fs.UnixNativeDispatcher;
import sun.nio.fs.UnixPath;

import java.io.*;
import java.lang.reflect.Field;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.ZipFile;


public class LeaderRecorder implements JMVXStrategy {
    // TODO add more fields as needed
    /*
     *Ahem* Allow me to explain:
     * toBuffer goes to the ring buffer (if active) otherwise the coordinator
     * toSocket always goes to the coordinator.
     * toCoordinator goes to the fastest communication mechanism
     *   When the ring buffer is active - it goes to the ring buffer
     *   Otherwise, it goes to the coordinator
     *   This variable REALLY should be renamed.
     */
    protected AFUNIXSocket leaderSocket;
    protected ObjectOutputStream toCoordinator;
    protected ObjectInputStream fromCoordinator;
    private ObjectOutputStream toSocketStream;
    private ObjectOutputStream toBufferStream;
    private ObjectInputStream fromSocketStream;
    private ObjectInputStream fromBufferStream;
    protected boolean connected = false;
    protected static Logger log;
    boolean promoteDemoteEnabled = true; // use promoteDemoteEnabled = ture, when testing a specific method
    private static final boolean fastExit = JMVXRuntime.parameters.fastExit;
    private static AFUNIXSocket clinitSocket;
    protected static ObjectOutputStream clinitOut;
    //use stacks in case one static init happens to trigger another
    protected Stack<ByteArrayOutputStream> clinitsByteStreams = new Stack();
    protected Stack<ObjectOutputStream> clinitsStreams = new Stack();
    protected int inClinit = 0;

    protected static int objectCounter = 0;

    private CRC32 crc32 = new CRC32();
    CircularBuffer buffer = null;

    protected static final boolean checksumCommunicationEnabled = JMVXRuntime.parameters.checksumCommunicationEnabled;
    protected static final boolean readsFromDisk = true; //force this to be on
    protected static final boolean circularBufferCommunicationEnabled = JMVXRuntime.parameters.circularBufferCommunicationEnabled;
    protected static final boolean avoidObjectSerialization = JMVXRuntime.parameters.avoidObjectSerialization;
    /* WHAT IS THE DIFFERENCE BETWEEN USE BUFFER BACKED STREAMS AND CIRCULAR BUFFER COMM ENABLED? */
    protected static final boolean useBufferBackedStreams = JMVXRuntime.parameters.useBufferBackedStreams;
    protected static final boolean validateWrites = JMVXRuntime.parameters.validateWrites;
    protected static final boolean computePartialOrders = JMVXRuntime.parameters.computePartialOrders;
    private static final int bufferSize = 10 * 1024 * 1024; //10 mb
    //protected static final String recordingDir = JMVXRuntime.parameters.recordingDir;
    /*map of path to jar -> path of jar on replay. Logs where we expect to find code
    JDK classes: path -> path
    Program classes: path -> $REC/path
    All entries with $REC/path will be copied into the recordingDir at the end of the Recording, so they will be
    available for replay. Note the Replayer will resolve redirected pathnames as needed
     */
    private static ConcurrentHashMap<String, String> jarsManifest = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Long, String> openedZips = new ConcurrentHashMap<>();
    //private static ObjectOutputStream manifests;
    private static Set<String> manifestsRecorded = new HashSet<>();
    //used to identify which jars come from JDK
    public static final String JAVA_HOME = System.getenv("JAVA_HOME");

    private static final AtomicBoolean isExecuting;

    private static final int junkPort = 10000;

    /* Used as a reference to the owner of an object for partial ordering */
    protected Object monitorOwnerRef = this;

    private Stack<String> classNames = new Stack();
    private Stack<ObjectOutputStream> classStreams = new Stack();

    private boolean workerThread = false;

    static {
        isExecuting = new AtomicBoolean();

        /**
         * Make and close a server to load libraries necessary for sockets and connections
         * The follower does the same thing, except it needs the server to simulate connections
         * If the code below is omitted, we will get divergences when performing loadLibrary calls
         */
        try {
            JMVXRuntime.enter();
            ServerSocket junk = new ServerSocket(junkPort);
            junk.close();
            //manifests = new ObjectOutputStream(maybeBufferedOutputStream(new FileOutputStream(Paths.get(recordingDir, "recording.manifests.dat").toFile())));
        } catch (IOException e) { //don't care server isn't used
            throw new Error(e);
        }finally {
            JMVXRuntime.exit();
        }

        PreloadClassUtility.preloadClasses();
    }

    protected int vectorClockIndex = -1;

    protected void sendMode() throws IOException {
        //Value is stored in Boolean lockstepMode on Coordinator
        toCoordinator.writeBoolean(false);
        toCoordinator.flush();
        toCoordinator.reset();
    }

    public static AtomicInteger one = new AtomicInteger(2);

    public void reConnect(AFUNIXSocket socket, ObjectOutputStream outputStream, ObjectInputStream inputStream, Logger l) {
        leaderSocket = socket;
        toCoordinator = outputStream;
        fromCoordinator = inputStream;
        log = l;

        // Make sure that new threads use the Leader strategy
        System.setProperty(JMVXRuntime.ROLE_PROP_NAME, "Leader");

        startSwitchingThread();
    }

    private static LeaderSwitchingThread switchingThread = null;

    private static void startSwitchingThread() {
        synchronized (Leader.class) {
            if (switchingThread == null || !switchingThread.isAlive()) {
                switchingThread = new LeaderSwitchingThread();
                switchingThread.start();
            }
        }

    }

    //private JMVXFastByteArray serializedObjectBytes = new JMVXFastByteArray();
    //private ObjectOutputStream serializedObject = null;
    private void writeToCircularBuffer(Object ob) {
        sendObjectToCoordinator((Serializable) ob);
        /*if(serializedObject == null) {
            //we are the only thread that uses this object, so we don't need any double check nonsense
            try {
                serializedObject = new ObjectOutputStream(serializedObjectBytes){
                    @Override
                    protected void writeStreamHeader() throws IOException {
                        //this would be written once, we want it at every object, look below for solution
                        //this.writeByte(OptimizedEvent.LENGTH.ordinal());
                    }
                };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            //fake the header
            serializedObjectBytes.write(OptimizedEvent.LENGTH.ordinal());
            serializedObject.writeObject(ob);
            serializedObject.reset();
            serializedObject.flush();
            serializedObjectBytes.writeToRingBuffer(buffer);
            //resuse the stream
            serializedObjectBytes.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
    }

    public static InputStream maybeBufferedInputStream(InputStream toDecorate){
        return useBufferBackedStreams ? new BufferedInputStream(toDecorate, bufferSize) : toDecorate;
    }

    public static OutputStream maybeBufferedOutputStream(OutputStream toDecorate){
        return useBufferBackedStreams ? new BufferedOutputStream(toDecorate, bufferSize) : toDecorate;
    }

    @Override
    public void main() {
        // Make sure ObjectOutputStream is fully initialized
        // This may involve reading file /dev/urandom
        try {
            new ObjectOutputStream(new ByteArrayOutputStream(8000)).writeObject(new Exception());
        } catch (IOException e) {
        }
        connect(1234);

        synchronized (Leader.class){
            if (!isExecuting.get()) {
                isExecuting.set(true);
                try {
                    toCoordinator.writeObject(CoordinatorThread.startMessage); //want coordinator thread to start it's switching thread
                    toCoordinator.flush();
                    fromCoordinator.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new Error(e);
                }
                startSwitchingThread();
                LeaderSwitchingThread.leaderThreads.clear();
            }
            LeaderSwitchingThread.addThreads(Thread.currentThread());
        }

        log = Logger.getLogger(Leader.class);

        log.info("Checksum communication enabled: " + checksumCommunicationEnabled);
        log.debug("Reading directly from disk: " + readsFromDisk);
        log.debug("Leader/Follower communication via circular buffer enabled: " + circularBufferCommunicationEnabled);
        log.debug("Avoiding object serialization enabled: " + avoidObjectSerialization);
        log.info("Buffer backed streams enabled: " + useBufferBackedStreams);

        if(circularBufferCommunicationEnabled) {
            buffer = JMVXRuntime.mmapFile(true);
//            this.sendObjectToCoordinator("WaitingForFollower");
//            String status = (String) this.receiveObjectFromCoordinator();
//            if (status.equals("FollowerReady")) {
            buffer.initializeOnBufferData();
            this.sendObjectToCoordinator("BufferInitialized");
//            } else {
//                throw new Error("Buffer initialization failed");
//            }

            try {
                toBufferStream = new ObjectOutputStream(new JMVXFastCircularBufferOutputStream(buffer));
                toBufferStream.flush();
                toBufferStream.reset();
            } catch (IOException e) {
                throw new Error(e);
            }
            toSocketStream = toCoordinator;
            toCoordinator = toBufferStream;
            fromSocketStream = fromCoordinator;
            try {
                this.fromCoordinator = new ObjectInputStream(new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException("The leader cannot read from the ring buffer");
                    }
                }) {
                    @Override
                    protected void readStreamHeader() throws IOException, StreamCorruptedException {
                        return;
                    }
                };
                fromBufferStream = this.fromCoordinator;
            } catch (IOException e) {
                throw new Error(e);
            }
        } else {
            toBufferStream = toCoordinator;
            toSocketStream = toCoordinator;
            fromSocketStream = fromCoordinator;
            fromBufferStream = fromCoordinator;
        }

        monitorOwnerRef = this;
        workerThread = Thread.currentThread() instanceof JMVXThreadFactory.JMVXPoolThread;
    }

    private static Object clinit = new Object();
    private Stack<Integer> vectorClockIndexHistory = new Stack<>();

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

    protected int loading = 0;

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name) throws ClassNotFoundException {
        return this.loadClass(loader, name, true);
    }

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name, boolean resolve) throws ClassNotFoundException {
        /* Mimic the passthrough of the Recorder
         * If this differs, class loading can diverge
         * URLClassLoaders are stateful. They cache input streams.
         * Data is loaded in and outside of class loading from jar files (code vs input files baked into the jar)
         * Loading a JMVX class with the Dacapo loader (or more generally application loader), will result in opening
         * additional jar files to search through.
         * Eventually, when getting a resource, the open operation will be omitted (because the stream is cached) and
         * thus an Open op is missing and the executions diverge.
         */
        try{
            //not logging these ops, FollowerRecorder will backfill
            JMVXRuntime.enter();
            loading++;
            if(name.startsWith("edu.uic.cs.jmvx") && ((ClassLoader)loader).getParent() != null) { //skip JMVX Classes
                return extLoader.$JMVX$loadClass(name, resolve);
            }else {
                return loader.$JMVX$loadClass(name, resolve);
            }
        }finally{
            loading--;
            JMVXRuntime.exit();
        }
    }

    @Override
    public void clinitStart(String cls) {
        //not logging these ops, FollowerRecorder will backfill
        JMVXRuntime.enter();
        inClinit++;
    }

    @Override
    public void clinitEnd(String cls) {
        inClinit--;
        JMVXRuntime.exit();
    }

    public void connect(int port) {
        if (!connected) {
            boolean coordinatorReady;
            vectorClockIndex = JMVXRuntime.clock.registerNewThread();
            while (true) {
                try {
                    //uncomment if using inDepthClinit
                    //setupClinitSocket();

                    leaderSocket = AFUNIXSocket.newInstance();
                    leaderSocket.connect(Coordinator.LEADER_COMMS_ADDR);
                    toCoordinator = new ObjectOutputStream(maybeBufferedOutputStream(leaderSocket.getOutputStream()));

                    toCoordinator.writeObject(Thread.currentThread().getName());
                    toCoordinator.writeInt(vectorClockIndex);
                    toCoordinator.flush();

                    fromCoordinator = new ObjectInputStream(maybeBufferedInputStream(leaderSocket.getInputStream()));
//                    clockIndex = fromCoordinator.readInt();

                    sendMode();
//                    coordinatorReady = fromCoordinator.readBoolean();
//                    coordinatorReady = fromCoordinator.readBoolean();
//                    while (!coordinatorReady) {
//                        try {
//                            coordinatorReady = fromCoordinator.readBoolean();
//                            Thread.sleep(100);
//                        } catch (InterruptedException ie) {
//                            ie.printStackTrace();
//                        }
//                    }
                    break;
                } catch (IOException e) {
                    //System.out.println("Leader to Coordinator Connection Failed. Retrying...");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            }
            connected = true;
        }
    }

    @Deprecated
    private synchronized void setupClinitSocket() throws IOException {
        //already made the socket, skip
        if(clinitSocket == null) {
            //make the clinit Socket
            clinitSocket = AFUNIXSocket.newInstance();
            clinitSocket.connect(Coordinator.LEADER_COMMS_ADDR);
            clinitOut = new ObjectOutputStream(clinitSocket.getOutputStream());

            clinitOut.writeObject("clinit");
            clinitOut.flush();
        }
    }

    /**
     * read 1
     * never used in batik
     *
     * @param is
     * @return
     * @throws IOException
     */
    @Override
    public int read(JMVXInputStream is) throws IOException {
        if(readsFromDisk){
            return is.$JMVX$read();
        }
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    return Optional.of(JMVXRuntime.read((InputStream) is));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(true);
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return (int) result.get();
            }
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

    /**
     * read 2
     * never used in batik
     *
     * @param is
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public int read(JMVXInputStream is, byte[] bytes) throws IOException {

        Callable<Optional<Object>> function = () -> {
            try {
                return Optional.of(JMVXRuntime.read((InputStream) is, bytes));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Optional.of(true);
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (int) result.get();
        }
        if (readsFromDisk && Utils.isInputStreamFromDisk(is)) {
            int bytesRead = is.$JMVX$read(bytes);
            //debug(is, ret, bytes.length);
            //Leader.debug(is, ret, bytes.length);

            /*int availableBytes = is.$JMVX$available();
            int spaceInBuffer = bytes.length - bytesRead;
            while (availableBytes > 0 && spaceInBuffer > 0) {
                bytesRead += is.$JMVX$read(bytes, bytesRead, Math.min(availableBytes, spaceInBuffer));
                availableBytes = is.$JMVX$available();
                spaceInBuffer = bytes.length - bytesRead;
            }*/
            return bytesRead;
        }
        try {
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

    /**
     * read 3
     *
     * @param is
     * @param bytes
     * @param off
     * @param len
     * @return
     * @throws IOException
     */
    @Override
    public int read(JMVXInputStream is, byte[] bytes, int off, int len) throws IOException {
        if (loading > 0 && is.getClass().equals(JMVXRuntime.zipFileInputStreamClass)) {
            return is.$JMVX$read(bytes, off, len);
        }

        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.read((InputStream) is, bytes, off, len));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (int) result.get();
        }

        if (readsFromDisk && Utils.isInputStreamFromDisk(is)) {
            /*int spaceInBuffer = Math.min(len, bytes.length - off);
            int bytesRead = is.$JMVX$read(bytes, off, len);
            // we take the minimum because if (len + off) > bytes.length, it will try to read more than
            // the capacity of the buffer
            spaceInBuffer -= bytesRead;
            int availableBytes = is.$JMVX$available();

            debug(is, bytesRead, bytes.length, off, len);

            // keep trying to read until we filled the buffer or there aren't more bytes available
            while (availableBytes > 0 && spaceInBuffer > 0) {
                // again, to avoid buffer overflow, we take the min of the number of bytes available
                // and the space left in the buffer
                bytesRead += is.$JMVX$read(bytes, bytesRead + off, Math.min(availableBytes, spaceInBuffer));
                availableBytes = is.$JMVX$available();
                spaceInBuffer -= bytesRead;
            }*/
            int bytesRead = is.$JMVX$read(bytes, off, len);
            return bytesRead;
        }
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

    private void sendTagOverBuffer(OptimizedEvent tag) {
        buffer.writeData(new byte[]{(byte) tag.ordinal()}, true);
    }

    @Override
    public int available(JMVXInputStream is) throws IOException {
        if(readsFromDisk){
            return is.$JMVX$available();
        }
        try {
            int ret = is.$JMVX$available();
            if (avoidObjectSerialization) {
                byte[] b = new byte[]{
                        (byte)(ret >>> 24),
                        (byte)(ret >>> 16),
                        (byte)(ret >>> 8),
                        (byte) ret
                };
                //THIS ISN'T WRITTEN DIRECTLY TO THE BUFFER?
                this.sendObjectToCoordinator(b);
            } else {
                Leader.Available a = new Leader.Available(ret);
                sendObjectToCoordinator(a);
            }
            return ret;
        } catch (IOException e) {
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void close(JMVXOutputStream os) throws IOException {
        try {
            os.$JMVX$close();
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public void flush(JMVXOutputStream os) throws IOException {
        try {
            os.$JMVX$flush();
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * write 1
     *
     * @param os
     * @param b
     * @throws IOException
     */
    @Override
    public void write(JMVXOutputStream os, int b) throws IOException {
        if(readsFromDisk){
            os.$JMVX$write(b);
            return;
        }
        try {
            // trigger the promote/demote process
            Callable<Optional<Object>> function = () -> {
                try {
                    JMVXRuntime.write((OutputStream) os, b);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return;
            }

            os.$JMVX$write(b);
            logWriteByte(b);
        } catch (IOException e) {
            logExceptionPossiblyWithSigil(e);
            throw e;
        }
    }

    private void logWriteByte(int b) {
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            if(validateWrites) {
                sendTagOverBuffer(OptimizedEvent.WRITE);
                byte data[] = new byte[Integer.BYTES];
                JMVXRuntime.unsafe.putInt(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, b);
                buffer.writeData(data, true);
            }else{
                sendTagOverBuffer(OptimizedEvent.WRITE_NO_VALIDATION);
            }
        } else {
            sendObjectToCoordinator(new Leader.Write(b));
        }
    }

    /** same as above, but with some types and tags swapped */
    private void logRandomAccessWriteByte(int b) {
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            if(validateWrites) {
                sendTagOverBuffer(OptimizedEvent.RANDOM_ACCESS_WRITE);
                byte data[] = new byte[Integer.BYTES];
                JMVXRuntime.unsafe.putInt(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, b);
                buffer.writeData(data, true);
            }else{
                sendTagOverBuffer(OptimizedEvent.WRITE_NO_VALIDATION);
            }
        } else {
            sendObjectToCoordinator(new Leader.RAWrite(b));
        }
    }

    /**
     * perform role switching
     * @param function lambda function
     * @param isEnabled default is false
     * @return DoNothingObject if role switching criteria are not matched.
     */
    private Optional<Object> switchRoles(Callable<Optional<Object>> function, boolean isEnabled) {
        if (one.get() == 1 && isEnabled) {
            sendObjectToCoordinator(new SwitchRolesEvent());
            Follower strategy = new Follower();
            strategy.reConnect(leaderSocket, toCoordinator, fromCoordinator, log);
            JMVXRuntime.setStrategy(ReentrantStrategy.singleLeaderPassthrough(strategy));
            JMVXRuntime.openExistingFilesToNull();
            try {
                return function.call();
            } catch (Exception e) {
                return Optional.of(e);
            }
        }
        return Optional.of(new DoNothingObject());
    }

    /**
     * write 2, never called in batik
     * @param os
     * @param bytes
     * @throws IOException
     */
    @Override
    public void write(JMVXOutputStream os, byte[] bytes) throws IOException {
        if(readsFromDisk){
            os.$JMVX$write(bytes);
            return;
        }
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    JMVXRuntime.write((OutputStream) os, bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return;
            }

            os.$JMVX$write(bytes);
            logWriteByteArray(bytes);
        } catch (IOException e) {
            logExceptionPossiblyWithSigil(e);
            throw e;
        }
    }

    private void logWriteByteArray(byte[] bytes) {
        byte[] bytesToSend = checksumCommunicationEnabled ? Utils.getChecksum(bytes, crc32) : bytes;
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            if(validateWrites) {
                sendTagOverBuffer(OptimizedEvent.WRITE);
                buffer.writeData(bytesToSend, true);
            }else{
                sendTagOverBuffer(OptimizedEvent.WRITE_NO_VALIDATION);
            }
        } else {
            sendObjectToCoordinator(new Leader.WriteB(bytesToSend));
        }
    }

    /** same as above, but with some types and tags swapped */
    private void logRandomAccessWriteByteArray(byte[] bytes) {
        byte[] bytesToSend = checksumCommunicationEnabled ? Utils.getChecksum(bytes, crc32) : bytes;
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            if(validateWrites) {
                sendTagOverBuffer(OptimizedEvent.RANDOM_ACCESS_WRITE);
                buffer.writeData(bytesToSend, true);
            }else{
                sendTagOverBuffer(OptimizedEvent.WRITE_NO_VALIDATION);
            }
        } else {
            sendObjectToCoordinator(new Leader.RAWriteB(bytesToSend));
        }
    }

    /**
     * write 3
     *
     * @param os
     * @param bytes
     * @param off
     * @param len
     * @throws IOException
     */
    @Override
    public void write(JMVXOutputStream os, byte[] bytes, int off, int len) throws IOException {
        if(readsFromDisk){
            os.$JMVX$write(bytes, off, len);
            return;
        }
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    JMVXRuntime.write((OutputStream) os, bytes, off, len);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return;
            }

            os.$JMVX$write(bytes, off, len);
            logWriteWithOffsetAndLength(bytes, off, len);
        } catch (IOException e) {
            logExceptionPossiblyWithSigil(e);
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
                sendTagOverBuffer(OptimizedEvent.EXCEPTION);
        }
        sendObjectToCoordinator(e);
    }

    private void logWriteWithOffsetAndLength(byte[] bytes, int off, int len) {
        byte[] bytesToSend = checksumCommunicationEnabled ? Utils.getChecksum(bytes, off, len, crc32) : bytes;
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            if(validateWrites) {
                sendTagOverBuffer(OptimizedEvent.WRITE);
                buffer.writeWriteOpData(bytesToSend, off, len, true);
            }else{
                sendTagOverBuffer(OptimizedEvent.WRITE_NO_VALIDATION);
            }
        } else {
            sendObjectToCoordinator(new Leader.WriteBII(bytesToSend, off, len));
        }
    }

    /** same as above, but with some types and tags swapped */
    private void logRandomAccessWriteWithOffsetAndLength(byte[] bytes, int off, int len) {
        byte[] bytesToSend = checksumCommunicationEnabled ? Utils.getChecksum(bytes, off, len, crc32) : bytes;
        if(circularBufferCommunicationEnabled && avoidObjectSerialization) {
            if(validateWrites) {
                sendTagOverBuffer(OptimizedEvent.RANDOM_ACCESS_WRITE);
                buffer.writeWriteOpData(bytesToSend, off, len, true);
            }else{
                sendTagOverBuffer(OptimizedEvent.WRITE_NO_VALIDATION);
            }
        } else {
            sendObjectToCoordinator(new Leader.RAWriteBII(bytesToSend, off, len));
        }
    }

    @Override
    public boolean canRead(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.canRead((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }

        boolean ret = f.$JMVX$canRead();
        this.sendObjectToCoordinator(new Leader.FileCanRead(ret));
        return ret;
    }

    /**
     * never reached in batik
     *
     * @param f
     * @return
     */
    @Override
    public boolean canWrite(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.canWrite((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }

        boolean ret = f.$JMVX$canWrite();
        this.sendObjectToCoordinator(new Leader.FileCanWrite(ret));
        return ret;
    }

    @Override
    public boolean createNewFile(JMVXFile f) throws IOException {
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    return Optional.of(JMVXRuntime.createNewFile((File) f));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return (boolean) result.get();
            }

            boolean ret = f.$JMVX$createNewFile();
            this.sendObjectToCoordinator(new Leader.FileCreateNewFile(ret));
            return ret;
        } catch (IOException e) {
            log.warn("createNewFile", e);
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public boolean delete(JMVXFile f) {
        if(loading > 0 || inClinit > 0){
            /* sync message is sent by the follower thread who is mapped to us!
             * if a benchmark ever triggers this, we will have to add a way to reroute the syncs
             */
            throw new UnsupportedOperationException("Won't be able to properly sync");
        }
        boolean ret = (((File) f).exists());
        this.sendObjectOverSocket(new Leader.FileDelete(ret));
        this.sendObjectOverSocket("waiting");
        String status = (String) this.receiveObjectOverSocket();

        if (status.equals("synced")) {
            ret = f.$JMVX$delete();
            this.sendObjectOverSocket(ret);
        } else {
            throw new Error("file delete failed");
        }
        return ret;
    }

    @Override
    public boolean exists(JMVXFile f) {
        if(loading > 0){
            return f.$JMVX$exists();
        }
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.exists((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }

        boolean ret = f.$JMVX$exists();
        this.sendObjectToCoordinator(new Leader.FileExists(ret));
        return ret;
    }

    /**
     * never used in batik
     *
     * @param f
     * @return
     */
    @Override
    public File getAbsoluteFile(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.getAbsoluteFile((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (File) result.get();
        }

        File ret = f.$JMVX$getAbsoluteFile();
        this.sendObjectToCoordinator(new Leader.FileGetAbsoluteFile(ret));
        return ret;
    }

    @Override
    public String getAbsolutePath(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.getAbsolutePath((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (String) result.get();
        }

        String ret = f.$JMVX$getAbsolutePath();
        this.sendObjectToCoordinator(new Leader.FileGetAbsolutePath(ret));
        return ret;
    }

    /**
     * never called in batik
     *
     * @param f
     * @return
     * @throws IOException
     */
    @Override
    public File getCanonicalFile(JMVXFile f) throws IOException {
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    return Optional.of(JMVXRuntime.getCanonicalFile((File) f));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return (File) result.get();
            }

            File ret = f.$JMVX$getCanonicalFile();
            this.sendObjectToCoordinator(new Leader.FileGetCanonicalFile(ret));
            return ret;
        } catch (IOException e) {
            log.warn("getCanonicalFile", e);
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public String getCanonicalPath(JMVXFile f) throws IOException {
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    return Optional.of(JMVXRuntime.getCanonicalPath((File) f));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return (String) result.get();
            }

            String ret = f.$JMVX$getCanonicalPath();
            this.sendObjectToCoordinator(new Leader.FileGetCanonicalPath(ret));
            return ret;
        } catch (IOException e) {
            log.warn("getCanonicalPath", e);
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public String getName(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.getName((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (String) result.get();
        }

        String ret = f.$JMVX$getName();
        this.sendObjectToCoordinator(new Leader.FileGetName(ret));
        return ret;
    }

    /**
     * never used in batik
     * @param f
     * @return
     */
    @Override
    public File getParentFile(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.getParentFile((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (File) result.get();
        }

        File ret = f.$JMVX$getParentFile();
        this.sendObjectToCoordinator(new Leader.FileGetParentFile(ret));
        return ret;
    }

    @Override
    public String getPath(JMVXFile f) {
        throw new UnsupportedOperationException("Shouldn't get here");
        /*Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.getPath((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (String) result.get();
        }

        String ret = f.$JMVX$getPath();
        this.sendObjectToCoordinator(new FileGetPath(ret));
        return ret;*/
    }

    @Override
    public boolean isDirectory(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.isDirectory((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }
        boolean ret = f.$JMVX$isDirectory();
        this.sendObjectToCoordinator(new Leader.FileIsDirectory(ret));
        return ret;
    }

    /**
     * never called in batik
     *
     * @param f
     * @return
     */
    @Override
    public long length(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.length((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (long) result.get();
        }

        long ret = f.$JMVX$length();
        this.sendObjectToCoordinator(new Leader.FileLength(ret));
        return ret;
    }

    @Override
    public String[] list(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.list((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (String[]) result.get();
        }
        String[] ret = f.$JMVX$list();
        this.sendObjectToCoordinator(new Leader.FileList(ret));
        return ret;
    }

    @Override
    public String[] list(JMVXFile f, FilenameFilter filter) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.list((File) f, filter));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (String[]) result.get();
        }
        String[] unfiltered = f.$JMVX$list();
        String[] ret = unfiltered;
        if (ret != null)
            ret = filterFiles(unfiltered, filter);
        //Send the unfiltered so the follower's filter can process them (and cause the same side effects)
        this.sendObjectToCoordinator(new Leader.FileListFilter(unfiltered));//luindex filter not serializable
        return ret;
    }

    @Override
    public File[] listFiles(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.listFiles((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (File[]) result.get();
        }
        File[] ret = f.$JMVX$listFiles();
        Leader.FileListFiles list = new Leader.FileListFiles(ret);
        if(circularBufferCommunicationEnabled){
            writeToCircularBuffer(list);
        }else {
            this.sendObjectToCoordinator(list);
        }
        return ret;
    }

    @Override
    public File[] listFiles(JMVXFile f, FilenameFilter filter) {
        //TODO add code for promote demote
        File[] unfiltered = f.$JMVX$listFiles();
        File[] ret = filterFiles(unfiltered, filter);
        //send the list of unfiltered files so the follower can apply the filter.
        //this is done to capture the side effects caused by the filter!
        Leader.FileListFilesFilter list = new Leader.FileListFilesFilter(unfiltered);
        if(circularBufferCommunicationEnabled){
            writeToCircularBuffer(list);
        }else {
            this.sendObjectToCoordinator(list);
        }
        return ret;
    }

    public static File[] filterFiles(File[] unfiltered, FilenameFilter filter) {
        ArrayList<File> files = new ArrayList<>();
        for (File file : unfiltered) {
            JMVXFile jf = (JMVXFile) file;
            if ((filter == null) || filter.accept(jf.$JMVX$getParentFile(), jf.$JMVX$getName())){
                files.add(file);
            }
        }
        return files.toArray(new File[files.size()]);
    }

    public static String[] filterFiles(String[] unfiltered, FilenameFilter filter) {
        ArrayList<String> files = new ArrayList<>();
        for (String filename : unfiltered) {
            JMVXFile jf = (JMVXFile) new File(filename);
            if ((filter == null) || filter.accept(jf.$JMVX$getParentFile(), jf.$JMVX$getName())){
                files.add(jf.$JMVX$getName());
            }
        }
        return files.toArray(new String[files.size()]);
    }

    @Override
    public boolean mkdir(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.mkdir((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }
        boolean ret = f.$JMVX$mkdir();
        this.sendObjectToCoordinator(new Leader.FileMkdir(ret));
        return ret;
    }

    @Override
    public boolean mkdirs(JMVXFile f) {
        boolean ret = f.$JMVX$mkdirs();
        this.sendObjectToCoordinator(new Leader.FileMkdirs(ret));
        return ret;
    }

    /**
     * never used in batik
     *
     * @param f
     * @param dest
     * @return
     */
    @Override
    public boolean renameTo(JMVXFile f, File dest) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.renameTo((File) f, dest));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }
        boolean ret = f.$JMVX$renameTo(dest);
        this.sendObjectToCoordinator(new Leader.FileRenameTo(ret, dest));
        return ret;
    }

    /**
     * never used in batik
     *
     * @param f
     * @return
     */
    @Override
    public boolean setReadOnly(JMVXFile f) {
        Callable<Optional<Object>> function = () -> {
            return Optional.of(JMVXRuntime.setReadOnly((File) f));
        };
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (boolean) result.get();
        }
        boolean ret = f.$JMVX$setReadOnly();
        this.sendObjectToCoordinator(new Leader.FileSetReadOnly(ret));
        return ret;
    }

    @Override
    public URL toURL(JMVXFile f) throws MalformedURLException {
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    return Optional.of(JMVXRuntime.toURL((File) f));
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return (URL) result.get();
            }
            URL ret = f.$JMVX$toURL();
            this.sendObjectToCoordinator(new Leader.FileToURL(ret));
            return ret;
        } catch (MalformedURLException e) {
            log.warn("toURL", e);
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long lastModified(JMVXFile f) {
        if(loading > 0){
            return f.$JMVX$lastModified();
        }
        Callable<Optional<Object>> function = () -> Optional.of(JMVXRuntime.lastModified((File) f));
        Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
        if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
            return (long) result.get();
        }

        long ret = f.$JMVX$lastModified();
        this.sendObjectToCoordinator(new Leader.FileLastModified(ret));
        return ret;
    }

    @Override
    public boolean isFile(JMVXFile f) {
        boolean ret = f.$JMVX$isFile();
        this.sendObjectToCoordinator(new Leader.IsFile(ret));
        return ret;
    }

    @Override
    public void open(JMVXFileOutputStream fos, String name, boolean append) throws FileNotFoundException {
        try {
            Callable<Optional<Object>> function = () -> {
                try {
                    JMVXRuntime.open((FileOutputStream) fos, name, append);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                return Optional.of(new VoidObject());
            };
            Optional<Object> result = switchRoles(function, promoteDemoteEnabled);
            if (result.isPresent() && !(result.get() instanceof DoNothingObject)) {
                return;
            }
            fos.$JMVX$open(name, append);
            JMVXRuntime.openedFiles.put(fos, name);
            this.sendObjectToCoordinator(new Leader.FileOutputStreamOpen(name, append));
        } catch (FileNotFoundException e) {
            log.warn("FileOutputStreamOpen", e);
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void open(JMVXFileInputStream fis, String name) throws FileNotFoundException {
        try{
            fis.$JMVX$open(name);
            this.sendObjectToCoordinator(new Leader.FileInputStreamOpen(name));
        } catch (FileNotFoundException e) {
            log.warn("FileInputStreamOpen", e);
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public String fileOutputStream(String name) {
        return name;
    }

    @Override
    public boolean fileOutputStream(boolean append) {
        return append;
    }

    @Override
    public File fileOutputStream(File file) {
        return file;
    }

    @Override
    public FileDescriptor fileOutputStream(FileDescriptor fdObj) {
        return fdObj;
    }

    @Override
    public void sync(FileDescriptor fd) throws SyncFailedException {
        try{
            fd.sync();
            this.sendObjectToCoordinator(new Leader.FileDescriptorSync());
        } catch (SyncFailedException e) {
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    protected void sendObjectToCoordinator(Serializable o) {
        try {
            objectCounter++;
            toCoordinator.writeObject(o);
            toCoordinator.flush();
            toCoordinator.reset();
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    protected void sendObjectOverSocket(Serializable o){
        try {
            objectCounter++;
            toSocketStream.writeObject(o);
            toSocketStream.flush();
            toSocketStream.reset();
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    private Object receiveObjectFromCoordinator() {
        Object o = null;
        try {
            o = fromCoordinator.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new Error(e);
        }
        return o;
    }

    private Object receiveObjectOverSocket() {
        Object o = null;
        try {
            o = fromSocketStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new Error(e);
        }
        return o;
    }

    public void connect(JMVXSocket sock, SocketAddress endpoint, int timeout) throws IOException{
        //Leader connects normally
        try {
            sock.$JMVX$connect(endpoint, timeout);
            //may need to refactor getFD to prevent exceptions from needing to be added...
            FileDescriptor fd = FileDescriptorUtils.getFileDescriptor((Socket) sock);
            leaderSocket.setOutboundFileDescriptors(fd);
            sendObjectOverSocket(new Leader.Connect(endpoint, timeout));
        } catch (IOException e) {
            //TODO catch other exceptions thrown by connect
            sendObjectToCoordinator(e);
            throw e;
        } catch (Exception e) {
            //(IllegalAccessException | NoSuchMethodException | InvocationTargetException | NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void bind(JMVXServerSocket serv, SocketAddress endpoint, int backlog) throws IOException {
        try {
            //bind to the endpoint
            serv.$JMVX$bind(endpoint, backlog);
            //get the fd and send it to the coordinator/follower
            FileDescriptor fd = FileDescriptorUtils.getFileDescriptor((ServerSocket) serv);
            leaderSocket.setOutboundFileDescriptors(fd);
            sendObjectOverSocket(new Leader.Bind(endpoint, backlog));
        }catch(IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public Socket accept(JMVXServerSocket serv) throws IOException {
        try {
            //get the new client socket
            Socket sock = serv.$JMVX$accept();
            //Get and send its file descriptor
            FileDescriptor fd = FileDescriptorUtils.getFileDescriptor(sock);
            leaderSocket.setOutboundFileDescriptors(fd);
            sendObjectOverSocket(new Leader.Accept());
            return sock;
        } catch (IOException e) {
            //TODO catch other exceptions thrown by connect
            sendObjectToCoordinator(e);
            throw e;
        } catch (Exception e) { //errors related to getting the fd...
            e.printStackTrace();
        }
        //Only get here if there is an error...
        return null;
    }

    private long[] clockCopy = new long[0];

    protected Object getMonitorOwnerReference(){
        return monitorOwnerRef;
    }

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
            /* REALLY IMPORTANT NOTE: TAKE THE SLOW PATH ON ANY OWNERSHIP CHANGE
             * EVEN WHEN GOING FROM UNOWNED TO OWNED
             * This creates an effect where the first monitorenter a thread or task does
             * goes through the vector clock, REGARDLESS OF WHAT OBJECT IS BEING LOCKED
             * This hurts the benefit of our attempts at flexible scheduling, but it allows us to safely
             * reuse slots in the vector clock, which saves on IO in critical sections.
             */
            //priorOwner==null means no one owned the monitor before
            if (priorOwner == ownerRef) {
                return true; //can take the fast path
            }else{
                u.setLastOwner(ownerRef);
            }
        }
        return false; //cannot take the fast path
    }

    /*
    Method is a bit of a hack.
    Rather than having 2 methods for monitorEnterFastPath and waitFastPath, there is one
    That method uses this one to figure out what type of object to serialize
     */
    protected Serializable serializeClockEventFromTag(OptimizedEvent tag, long[] clockCopy){
        switch (tag){
            case MONITOR_ENTER:
            case FAST_MONITOR_ENTER:
                return new Leader.MonitorEnter(clockCopy, tag);
            case WAIT:
            case FAST_WAIT:
                return new Leader.Wait(clockCopy, 0, tag);
            default:
                throw new Error("Cannot create an object for a vc event from " + tag.name());
        }
    }

    public static final long[] NO_CLOCK = new long[0];

    /* Log a monitor fast path */
    protected void writeClockFastPath(OptimizedEvent fastTag) {
        if(circularBufferCommunicationEnabled) {
            if(avoidObjectSerialization) {
                buffer.writeVectorClockData((byte)fastTag.ordinal(), NO_CLOCK, true);
            }
            else {
                this.writeToCircularBuffer(serializeClockEventFromTag(fastTag, null));
            }
        }
        else {
            this.sendObjectToCoordinator(serializeClockEventFromTag(fastTag, null));
        }
    }

    /* Log a monitor slow path */
    protected void writeClockSlowPath(OptimizedEvent slowTag, long[] newClockCopy){
        if(circularBufferCommunicationEnabled) {
            if(avoidObjectSerialization) {
                buffer.writeVectorClockData((byte)slowTag.ordinal(), newClockCopy, true);
            }
            else {
                this.writeToCircularBuffer(serializeClockEventFromTag(slowTag, newClockCopy));
            }
        }
        else {
            this.sendObjectToCoordinator(serializeClockEventFromTag(slowTag, newClockCopy));
        }
    }

    @Override
    public void monitorenter(Object o) {
        if (inClinit > 0 || loading > 0) {
            // Don't log monitors inside class initializers
            // These should be able to be run by any thread
            // Just get the lock
            JMVXRuntime.unsafe.monitorEnter(o);
            return;
        }

        VectorClock clock = JMVXRuntime.clock;
        JMVXRuntime.unsafe.monitorEnter(o);

        if(canTakeClockFastPath(o)){
            clock.increment(vectorClockIndex, null);
            writeClockFastPath(OptimizedEvent.FAST_MONITOR_ENTER);
        }else{
            clockCopy = clock.increment(vectorClockIndex, clockCopy);
            writeClockSlowPath(OptimizedEvent.MONITOR_ENTER, clockCopy);
        }
    }

    @Override
    public void monitorexit(Object o) {
        JMVXRuntime.unsafe.monitorExit(o);
    }

    //TODO/NOTE: should modify to handle when inClinit > 0
    //we don't want to modify the vector clock in that case (or send clock objects)
    @Override
    public void wait(Object o, long timeout, int nanos) throws InterruptedException {
        if (inClinit > 0 || loading > 0) {
            // Don't log monitors inside class initializers
            // These should be able to be run by any thread
            // Just get the lock
            o.wait(timeout, nanos);
            return;
        }
        VectorClock clock = JMVXRuntime.clock;
        o.wait(timeout, nanos);

        if(canTakeClockFastPath(o)){
            clock.increment(vectorClockIndex, null);
            writeClockFastPath(OptimizedEvent.FAST_WAIT);
        }else{
            clockCopy = clock.increment(vectorClockIndex, clockCopy);
            writeClockSlowPath(OptimizedEvent.WAIT, clockCopy);
        }
    }

    @Override
    public void threadStart(Runnable r) {

    }

    @Override
    public void threadExit(Runnable r) {
        JMVXRuntime.clock.unregisterThread(vectorClockIndex);
        exitLogic();
    }

    @Override
    public void exitLogic() {
        try {
            /*
            We don't need to send "RTE" to cleanly exit--we could just break the connection
            Not doing that to determine why the connection was terminated.
            E.g., disconnecting can come from crashing or exiting. The "RTE" message informs us the disconnect
            was from an exit; it also nicely mimics the Followers logic.
             */
            LeaderSwitchingThread.removeThreads(Thread.currentThread());
            //fail safe to prevent a follower thread from blocking indefinitely while waiting for a task
            if(workerThread){
                /* THIS SHOULD NOT BE SENT TO THE COORDINATOR
                 * we want this to be buffered in the rest of the event history */
                sendObjectToCoordinator(CoordinatorThread.exitMessage);
            }
            if (useBufferBackedStreams) {
                toCoordinator = toSocketStream;
                fromCoordinator = fromSocketStream;
            }
            if (LeaderSwitchingThread.leaderThreads.size() == 0){
                toCoordinator.writeObject(CoordinatorThread.finalExitMessage); //Last thread is exiting
                isExecuting.set(false);
            }
            else
                toCoordinator.writeObject(CoordinatorThread.exitMessage); //want coordinator thread to start listening to the follower
            toCoordinator.flush();

            /*if(Thread.currentThread() instanceof JMVXThreadFactory.JMVXPoolThread){
                toCoordinator.
            }*/

            if(!fastExit) {
                //wait for the coordinator to tell us the follower has exited
                fromCoordinator.readObject(); //wait for ACK
            }
            if (useBufferBackedStreams) {
                toCoordinator = toBufferStream;
                fromCoordinator = fromBufferStream;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final boolean DEBUG = (System.getProperty("IS_DEBUG") != null);

    /*default*/
    static void debug(JMVXInputStream is, int ret) {
        if (DEBUG)
            debug(is, ret, Optional.empty(), Optional.empty(), Optional.empty());
    }

    /*default*/
    static void debug(JMVXInputStream is, int ret, int size) {
        if (DEBUG)
            debug(is, ret, Optional.of(size), Optional.empty(), Optional.empty());
    }

    /*default*/
    static void debug(JMVXInputStream is, int ret, int size, int off, int len) {
        if (DEBUG)
            debug(is, ret, Optional.of(size), Optional.of(off), Optional.of(len));
    }

    private static void debug(JMVXInputStream is, int ret, Optional<Integer> size, Optional<Integer> off, Optional<Integer> len) {
        String name = "";
        if (is instanceof FileInputStream) {
            FileInputStream fis = (FileInputStream) is;
            try {
                Field f = FileInputStream.class.getDeclaredField("path");
                f.setAccessible(true);
                name = (String) f.get(fis);
            } catch (ReflectiveOperationException ex) {
                throw new Error(ex);
            }
        } else if (is.getClass().getSimpleName().endsWith("ZipFileInputStream")) {
            try {
                Field outerField = is.getClass().getDeclaredField("this$0");
                outerField.setAccessible(true);
                ZipFile outer = (ZipFile) outerField.get(is);
                name = outer.getName();
            } catch (ReflectiveOperationException ex) {
                throw new Error(ex);
            }
        } else {
            name = is.getClass().getName();
        }

        String o = off.map(i -> "offset: " + i + " ").orElse("");
        String s = size.map(i -> "size: " + i + " ").orElse("");
        String l = len.map(i -> "length: " + i + " ").orElse("");

        System.out.println("" + o + l + s + ret + " " + name);
    }

    public static void debug(JMVXRandomAccessFile f, int ret, Optional<Integer> size, Optional<Integer> off, Optional<Integer> len) throws IOException {
        if(DEBUG) {
            System.out.format("(RAF) FD: %s offset: %s size: %s length: %s\n",
                    ((RandomAccessFile) f).getFD().toString(),
                    off.map(i -> i.toString()).orElse("-"),
                    size.map(i -> i.toString()).orElse("-"),
                    len.map(i -> i.toString()).orElse("-"));
        }
    }

    private ReadRecorderUtility readRecorder = new ReadRecorderUtility(avoidObjectSerialization, circularBufferCommunicationEnabled, true);

    @Override
    public int read(JMVXRandomAccessFile f) throws IOException {
        if(isSkippableRandomAccessRead(f)){ //fast path read
            //outside the exception handler on purpose
            //if we are willing to skip the read, we skip logging the exception too
            return f.$JMVX$read();
        }
        try {
            int ret = f.$JMVX$read();
            readRecorder.recordReadByte(toCoordinator, buffer, OptimizedEvent.RANDOM_ACCESS_READ, ret);
            return ret;
        } catch (IOException e) {
            readRecorder.logException(toCoordinator, e);
            throw e;
        }
    }

    private boolean isSkippableRandomAccessRead(JMVXRandomAccessFile f) {
        return readsFromDisk && fastPathRandomAccessFiles.get(f) != null;
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes) throws IOException {
        if(isSkippableRandomAccessRead(f)){ //fast path read
            return f.$JMVX$read(bytes);
        }
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
        if(isSkippableRandomAccessRead(f)){ //fast path read
            return f.$JMVX$read(bytes, off, len);
        }
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

    //really going to use it as a set
    private static ConcurrentHashMap<JMVXRandomAccessFile, Integer> fastPathRandomAccessFiles = new ConcurrentHashMap<>();
    private static final int RD_ONLY_FLAG = 1;

    @Override
    public void open(JMVXRandomAccessFile raf, String name, int mode) throws FileNotFoundException {
        try {
            if((mode & RD_ONLY_FLAG) != 0){
                fastPathRandomAccessFiles.put(raf, 1);
            }
            raf.$JMVX$open(name, mode);
            this.sendObjectToCoordinator(new Leader.RAOpen(name, mode));
        } catch (FileNotFoundException e) {
            this.sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void close(JMVXRandomAccessFile raf) throws IOException {
        try {
            raf.$JMVX$close();
        } catch (IOException e) {
            throw e;
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

    public long length(JMVXRandomAccessFile f) throws IOException {
        try{
            long len = ((RandomAccessFile)f).length();
            sendObjectToCoordinator(new Leader.RALength(len));
            return len;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void setLength(JMVXRandomAccessFile f, long newLength) throws IOException {
        try{
            ((RandomAccessFile)f).setLength(newLength);
            sendObjectToCoordinator(new Leader.RASetLength(newLength));
        }catch (IOException e){
            sendObjectToCoordinator(e);
        }
    }

    @Override
    public void seek(JMVXRandomAccessFile f, long pos) throws IOException {

        if(isSkippableRandomAccessRead(f)){
            f.$JMVX$seek(pos);
            return;
        }

        if (circularBufferCommunicationEnabled) {
            try {
                f.$JMVX$seek(pos);
                if (avoidObjectSerialization) {
                    byte[] data = new byte[] {
                            (byte)  pos,
                            (byte) (pos >> 8),
                            (byte) (pos >> 16),
                            (byte) (pos >> 24),
                            (byte) (pos >> 32),
                            (byte) (pos >> 40),
                            (byte) (pos >> 48),
                            (byte) (pos >> 56)};
                    buffer.writeWriteOpData(data, 0, 8, true);
                } else {
                    Leader.RASeek raseek = new Leader.RASeek(pos);
                    this.writeToCircularBuffer(raseek);
                }
            } catch (IOException e) {
                throw new Error("Not supported");
            }
        } else {
            try{
                f.$JMVX$seek(pos);
                sendObjectToCoordinator(new Leader.RASeek(pos));
            }catch (IOException e){
                sendObjectToCoordinator(e);
            }
        }

    }

    @Override
    public int read(JMVXFileChannel c, ByteBuffer dst) throws IOException {
        //TODO: use the readRecorder here!
        try{
            int ret = c.$JMVX$read(dst);
            /*ByteBuffer.array is an OPTIONAL METHOD.
            The methods available depends on the underlying mechanism.
            E.g., HeapBuffer versus DirectBuffer*/
            byte[] buf;
            if(dst.isDirect()){
                //memory is outside the JVM, we need to pull it in
                //ret is the number of bytes read!
                buf = new byte[ret];
                dst.rewind();
                dst.get(buf);
            }else{
                //memory resides on the heap, we can access it easily!
                buf = dst.array();
            }
            Leader.ChannelRead read = new Leader.ChannelRead(buf, ret);
            if(circularBufferCommunicationEnabled){
                writeToCircularBuffer(read);
            }else {
                sendObjectToCoordinator(read);
            }
            return ret;
        }catch(IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long size(JMVXFileChannel c) throws IOException {
        try {
            long ret = c.$JMVX$size();
            sendObjectToCoordinator(new Leader.ChannelSize(ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long currentTimeMillis() {
        long ret = System.currentTimeMillis();

        if (avoidObjectSerialization && circularBufferCommunicationEnabled) {
            byte[] b = new byte[] {
                    (byte)  ret,
                    (byte) (ret >> 8),
                    (byte) (ret >> 16),
                    (byte) (ret >> 24),
                    (byte) (ret >> 32),
                    (byte) (ret >> 40),
                    (byte) (ret >> 48),
                    (byte) (ret >> 56)};
            this.buffer.writeData(b, 0, b.length);
//            sendObjectToCoordinator(b);
        } else {
            sendObjectToCoordinator(new Leader.TimeMillis(ret));
        }

        return ret;
    }

    @Override
    public long nanoTime() {
        long ret = System.nanoTime();
        sendObjectToCoordinator(new Leader.NanoTime(ret));
        return ret;
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix, FileAttribute<?>... attrs) throws IOException {
        Path ret = Files.createTempFile(dir, prefix, suffix, attrs);
        sendObjectToCoordinator(new Leader.CreateTempFile(ret.toFile(), null, prefix, suffix, attrs));
        return ret;
    }

    @Override
    public Path createTempFile(String prefix, String suffix, FileAttribute<?>... attrs) throws IOException {
        Path ret = Files.createTempFile(prefix, suffix, attrs);
        sendObjectToCoordinator(new Leader.CreateTempFile(ret.toFile(), null, prefix, suffix, attrs));
        return ret;
    }

    @Override
    public void copy(JMVXFileSystemProvider fsp, Path source, Path target, CopyOption... options) throws IOException {
        fsp.$JMVX$copy(source, target, options);
        sendObjectToCoordinator(new Leader.FileSystemProviderCopy(source.toFile().getPath(), target.toFile().getPath()));
    }

    @Override
    public void checkAccess(JMVXFileSystemProvider fsp, Path path, AccessMode... modes) throws IOException {
        try {
            fsp.$JMVX$checkAccess(path, modes);
            sendObjectToCoordinator(new Leader.FileSystemProviderCheckAccess(null));
        } catch (IOException e) {
            sendObjectToCoordinator(new Leader.FileSystemProviderCheckAccess(e));
            throw e;
        }
    }

    @Override
    public int open(UnixPath path, int flags, int mode) throws UnixException {
        try {
            int ret = UnixNativeDispatcher.open(path, flags, mode);
            sendObjectToCoordinator(new Leader.NativeOpen(path.toString(), flags, mode, ret));
            return ret;
        }catch (UnixException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void stat(UnixPath path, UnixFileAttributes attrs) throws UnixException {
        try {
            UnixNativeDispatcher.stat(path, attrs);
            sendObjectToCoordinator(new Leader.NativeStat(path.toString(), attrs));
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void lstat(UnixPath path, UnixFileAttributes attrs) throws UnixException {
        try {
            UnixNativeDispatcher.lstat(path, attrs);
            sendObjectToCoordinator(new Leader.NativeLstat(path.toString(), attrs));
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long opendir(UnixPath path) throws UnixException {
        try {
            long ret = UnixNativeDispatcher.opendir(path);
            sendObjectToCoordinator(new Leader.NativeOpenDir(path.toString(), ret));
            return ret;
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public byte[] readdir(long dir) throws UnixException {
        try {
            byte[] ret = UnixNativeDispatcher.readdir(dir);
            sendObjectToCoordinator(new Leader.NativeReadDir(dir, ret));
            return ret;
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void access(UnixPath path, int amode) throws UnixException {
        try {
            UnixNativeDispatcher.access(path, amode);
            sendObjectToCoordinator(new Leader.NativeAccess(path.toString(), amode));
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void closedir(long dir) throws UnixException {
        UnixNativeDispatcher.closedir(dir);
    }

    @Override
    public void mkdir(UnixPath path, int mode) throws UnixException {
        try {
            UnixNativeDispatcher.mkdir(path, mode);
            sendObjectToCoordinator(new Leader.NativeMkdir(path.toString(), mode));
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int dup(int fd) throws UnixException {
        try {
            int ret = UnixNativeDispatcher.dup(fd);
            sendObjectToCoordinator(new Leader.NativeDup(fd, ret));
            return ret;
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long fdopendir(int dfd) throws UnixException {
        try {
            long ret = UnixNativeDispatcher.fdopendir(dfd);
            sendObjectToCoordinator(new Leader.NativeFdOpenDir(dfd, ret));
            return ret;
        }catch (Exception e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public byte[] realpath(UnixPath path) throws UnixException {
        try{
            byte[] ret = UnixNativeDispatcher.realpath(path);
            sendObjectToCoordinator(new Leader.NativeRealPath(path.toString(), ret));
            return ret;
        }catch (UnixException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int read(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        try{
            int ret = impl.$JMVX$read(fd, address, len);
            sendObjectToCoordinator(new Leader.FileDispatcherRead(FileDescriptorUtils.fdAccess.get(fd), address, len, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int pread(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len, long position) throws IOException {
        try{
            int ret = impl.$JMVX$pread(fd, address, len, position);
            sendObjectToCoordinator(new Leader.FileDispatcherPread(FileDescriptorUtils.fdAccess.get(fd), address, len, ret, position));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long readv(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        try{
            long ret = impl.$JMVX$readv(fd, address, len);
            sendObjectToCoordinator(new Leader.FileDispatcherReadv(FileDescriptorUtils.fdAccess.get(fd), address, len));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int write(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        try{
            int ret = impl.$JMVX$write(fd, address, len);
            sendObjectToCoordinator(new Leader.FileDispatcherWrite(FileDescriptorUtils.fdAccess.get(fd), address, len, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int pwrite(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len, long position) throws IOException {
        try{
            int ret = impl.$JMVX$pwrite(fd, address, len, position);
            sendObjectToCoordinator(new Leader.FileDispatcherPwrite(FileDescriptorUtils.fdAccess.get(fd), address, len, position, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long writev(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        try{
            long ret = impl.$JMVX$writev(fd, address, len);
            sendObjectToCoordinator(new Leader.FileDispatcherWritev(FileDescriptorUtils.fdAccess.get(fd), address, len, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long seek(JMVXFileDispatcherImpl impl, FileDescriptor fd, long offset) throws IOException {
        try{
            long ret = impl.$JMVX$seek(fd, offset);
            sendObjectToCoordinator(new Leader.FileDispatcherSeek(FileDescriptorUtils.fdAccess.get(fd), offset, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int force(JMVXFileDispatcherImpl impl, FileDescriptor fd, boolean metaData) throws IOException {
        try{
            int ret = impl.$JMVX$force(fd, metaData);
            sendObjectToCoordinator(new Leader.FileDispatcherForce(FileDescriptorUtils.fdAccess.get(fd), metaData, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int truncate(JMVXFileDispatcherImpl impl, FileDescriptor fd, long size) throws IOException {
        try{
            int ret = impl.$JMVX$truncate(fd,size);
            sendObjectToCoordinator(new Leader.FileDispatcherTruncate(FileDescriptorUtils.fdAccess.get(fd), size));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public long size(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        try{
            long ret = impl.$JMVX$size(fd);
            sendObjectToCoordinator(new Leader.FileDispatcherSize(FileDescriptorUtils.fdAccess.get(fd), ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public int lock(JMVXFileDispatcherImpl impl, FileDescriptor fd, boolean blocking, long pos, long size, boolean shared) throws IOException {
        try{
            int ret = impl.$JMVX$lock(fd, blocking, pos, size, shared);
            sendObjectToCoordinator(new Leader.FileDispatcherLock(FileDescriptorUtils.fdAccess.get(fd), blocking, pos, size, shared, ret));
            return ret;
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void release(JMVXFileDispatcherImpl impl, FileDescriptor fd, long pos, long size) throws IOException {
        try{
            impl.$JMVX$release(fd, pos, size);
            sendObjectToCoordinator(new Leader.FileDispatcherRelease(FileDescriptorUtils.fdAccess.get(fd), pos, size));
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public void close(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        impl.$JMVX$close(fd);
    }

    @Override
    public void preClose(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        try{
            impl.$JMVX$preClose(fd);
            sendObjectToCoordinator(new Leader.FileDispatcherPreClose(FileDescriptorUtils.fdAccess.get(fd)));
        }catch (IOException e){
            sendObjectToCoordinator(e);
            throw e;
        }
    }

    @Override
    public String getSystemTimeZoneID(String javaHome) {
        String ret = TimeZone.getSystemTimeZoneID(javaHome);
        sendObjectToCoordinator(new Leader.GetSystemTimeZoneID(javaHome, ret));
        return ret;
    }

    @Override
    public ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
        JMVXThreadPoolExecutor pool = new PessimisticExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
        return pool;
    }

    @Override
    public ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        /* linked blocking queue won't obey the stamps on the jobs
         * but the follower doesn't need the order to be explicitly followed
         * because the leader will tell the follower's worker which job to take, based on the stamp */
        JMVXThreadPoolExecutor pool = new JMVXThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
        return pool;
    }

    @Override
    public void beforeExecute(Thread t, JMVXJob r) {
        //send the stamp of our job to the follower
        //this is the job we get back from the scheduler
        monitorOwnerRef = r;
        sendObjectToCoordinator(new Leader.JobStarted(r.$JMVX$getClock()));
    }

    @Override
    public void afterExecute(JMVXJob r, Throwable t) {
        /* Maybe we can just run? No stream state to reset */
        monitorOwnerRef = this;
    }

    @Override
    public void submit(JMVXJob task) {
        //send stamp we created to the follower
        int workerId = task.$JMVX$getClock();
        sendObjectToCoordinator(new RecorderWithPools.JobCreated(workerId));
    }

    @Override
    public void runnableFutureDone(JMVXQueueingFuture future) {
        //send stamp we created to the follower
        int stamp = future.tick();
        sendObjectToCoordinator(new RecorderWithPools.TaskFinished(stamp));
    }

    /* AUGMENTED FROM RECORDER
     *  loading check is redundant because the leader enters when loading
     *  so all of that logic would go through the SingleLeader*/
}
