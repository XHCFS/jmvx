package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.Utils;
import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.coordinate.Coordinator;
import edu.uic.cs.jmvx.coordinate.CoordinatorThread;
import edu.uic.cs.jmvx.runtime.*;
import edu.uic.cs.jmvx.runtime.pools.*;
import edu.uic.cs.jmvx.runtime.util.JMVXFastCircularBufferInputStream;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;
import edu.uic.cs.jmvx.runtime.util.PreloadClassUtility;
import edu.uic.cs.jmvx.runtime.util.SeparateLoadingLogs;
import edu.uic.cs.jmvx.runtime.util.recording.ReadRecorderUtility;
import edu.uic.cs.jmvx.runtime.util.recording.ReadReplayerUtility;
import org.apache.log4j.Logger;
import org.newsclub.net.unix.AFUNIXSocket;
import sun.misc.Unsafe;
import sun.nio.fs.UnixException;
import sun.nio.fs.UnixFileAttributes;
import sun.nio.fs.UnixNativeDispatcher;
import sun.nio.fs.UnixPath;

import java.io.*;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * Goal: Leader -> Follower/Recorder
 * The leader gets to run fast because the IO bottleneck of recording is moved to the follower
 * To increase speed more, the follower will use the readsFromDisk optimization
 * So the leader only needs to send read metadata, rather than the bytes
 *
 * A big issue is that this call is the epitome of a diamond problem
 *           JMVXStrategy
 *            /        \
 *         Leader     Follower
 *           |          |
 *        Recorder      |
 *           \          /
 *         Follower/Recorder
 *
 * Furthermore, we really want to splice methods.
 * The follower should receive and event, check for divergences, handle a read with readsFromDisk
 * and the recorder should JUST record.
 * However, the recorder ALWAYS executes the effect (eg the read) before logging.
 * There is no level of indirection, both happen in the same method call.
 * This is why we cannot initialize 2 strategies (follower and recorder) and execute both
 * The Recorder has side effects--eg it will write data!
 * The Follower will also write data in some circumstances.
 * So `System.out.println("Hello") -> "HHeelloo"
 *
 * A better strategy would be to build strategies out of mixins for:
 *   1. Executing an effect
 *   2. Encoding an event/logging
 *   3. Checking for divergences
 *   4. Receiving and event/decoding
 *
 * The leader/recorder uses mixins 1,2
 * The follower/replayer uses mixins 3,4
 * Follower/Recorder uses them all.
 *
 * Unfortunately, JMVX was not build with this idea in mind.
 * As a prototype, I'm going to be manually coding what the result of a mixin would be.
 * God help me. >.<
 */
public class FollowerRecorderImpl implements JMVXStrategy {

    //used to store a reference to the task stream for worker threads in thread pools
    private Stack<ObjectOutputStream> logFileObjectStreams = new Stack<>();

    //private static volatile int threadsAlive = 1; //main thread is 1

    private static final String recordingDir = JMVXRuntime.parameters.recordingDir;
    protected ObjectOutputStream toLogFile;
    protected static ConcurrentHashMap<String, Flushable> streamsToFlush = new ConcurrentHashMap<>();
    protected static final boolean delayedWriteRecording = JMVXRuntime.parameters.delayedWriteRecording;
    protected static final boolean markElidedEvents = JMVXRuntime.parameters.markElidedEvents;


    private int loading = 0;
    private Stack<Integer> vectorClockIndexHistory = new Stack<>();
    //I should probably get rid of this...
    //private Recorder recorder = new RecorderWithPools();

    private static final Path EMPTY_DIR;
    static {
        try {
            EMPTY_DIR = Files.createTempDirectory("JMVXEmptyDir");
        } catch (IOException e) {
            throw new Error(e);
        }

        PreloadClassUtility.preloadClasses();
    }

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name) throws ClassNotFoundException {
        throw new UnsupportedOperationException("Not implemented, should be redirected by BadIdea");
    }

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name, boolean resolve) throws ClassNotFoundException {
        throw new UnsupportedOperationException("Not implemented, should be redirected by BadIdea");
    }

    @Deprecated
    private JMVXFile waitUntilFileExists(Path p) {
        try {
            JMVXRuntime.enter(); //just in case
            JMVXFile f = (JMVXFile) p.toFile();
            /* wait for the leader to write the log file
             * NOTE: can this race with the flushing of a file?
             * Need to have a file lock of some sort. */
            while (!f.$JMVX$exists()) {
                long waitUntil = System.currentTimeMillis() + 100;
                while (waitUntil > System.currentTimeMillis()) ;
            }
            return f;
        }finally {
            JMVXRuntime.exit();
        }
    }

    @Override
    public void clinitStart(String cls) {
        throw new UnsupportedOperationException("Not implemented, should be redirected by BadIdea");
    }

    @Override
    public void clinitEnd(String cls) {
        throw new UnsupportedOperationException("Not implemented, should be redirected by BadIdea");
    }

    protected byte[] getClinit(String cls) {
        byte[] bytes = null;
        Path p = SeparateLoadingLogs.clinitLogPath(cls);
        File f = (File)waitUntilFileExists(p);
        boolean retry = false;
        do {
            try (ObjectInputStream in = new ObjectInputStream(maybeBufferedInputStream(new FileInputStream(f)))) {
                in.readObject(); //class name
                bytes = (byte[]) in.readObject();
                retry = false;
            }catch (StreamCorruptedException e){
                retry = true;
            }catch (IOException exception) {
                retry = true;
                //throw new DivergenceError();
            } catch (ClassNotFoundException e) {
                throw new DivergenceError();
            }
        }while(retry);
        return bytes;
    }

    @Override
    public void initIDs() {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long getEntry(long jzfile, byte[] name) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public void freeEntry(long jzfile, long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long getNextEntry(long jzfile, int i) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public void close(long jzfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long open(String name, int mode, long lastModified, boolean usemmap) throws IOException {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public int getTotal(long jzfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public int getManifestNum(long jzfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public boolean startsWithLOC(long jzfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public int read(long jzfile, long jzentry, long pos, byte[] b, int off, int len) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long getEntryTime(long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long getEntryCrc(long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long getEntryCSize(long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public long getEntrySize(long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public int getEntryMethod(long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public int getEntryFlag(long jzentry) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public byte[] getCommentBytes(long jzfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public byte[] getEntryBytes(long jzentry, int type) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public String getZipMessage(long jzfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    @Override
    public String[] getMetaInfEntryNames(JMVXJarFile jarfile) {
        throw new UnsupportedOperationException("Should be diverted to recorder");
    }

    private ConcurrentHashMap<JMVXFile, WeakReference<JMVXFile>> redirectedFiles = new ConcurrentHashMap<>();

    /*
    Redirects files which are to be used for loading
     */
    public JMVXFile redirect(JMVXFile f){
        //don't redirect here, we're opening the files.
        return f;
    }

    protected AFUNIXSocket followerSocket;
    protected ObjectOutputStream toCoordinator;
    protected ObjectInputStream fromCoordinator;
    private ObjectInputStream fromSocketStream;
    private ObjectInputStream fromBufferStream;
    private ObjectOutputStream toSocketStream;
    private ObjectOutputStream toBufferStream;

    //Address of the server socket we use to emulate connections
    protected static final int junkPort = JMVXRuntime.parameters.junkPort;
    protected final static InetSocketAddress junkAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), junkPort);
    protected static ServerSocket junkServer;
    protected boolean connected = false;

    protected static Logger log;

    private static AFUNIXSocket clinitSocket;
    //private static ObjectOutputStream clinitOut;
    //private static ObjectInputStream clinitIn;
    //doesn't need to be a thread local because we sync on the class.
    protected Stack<ObjectOutputStream> clinitsStreams = new Stack();
    //value to tell if we should modify the vectorclock on monitorenter (do if = 0)
    //thread local because not all threads will be in a clinit
    //those threads will need to use the original monitorenter
    protected int inClinit = 0;

    private static List<AFUNIXSocket> connections = new ArrayList<>();

    protected static int objectCounter = 0;

    private CRC32 crc32 = new CRC32();
    CircularBuffer buffer = null;
    int one = 0;

    // Using the default DivergenceHandler for now, but later can be changed to a user-supplied one
    DivergenceHandler divergenceHandler;
    protected int vectorClockIndex = -1;
    /* is the role is attached to a worker thread */
    protected boolean workerThread = false;
    /* reference to the running thread or task (if a worker).
     * Used as a reference to the owner of an object for partial ordering */
    //protected Runnable taskRef = null;

    {
        try {
            String className = JMVXRuntime.parameters.divergenceHandlerClassName;
            Class<?> clazz = null;
            clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor();
            divergenceHandler = (DivergenceHandler) constructor.newInstance();
        } catch (Exception e) {
            divergenceHandler = new DacapoDivergenceHandler();
        }
    }

    static{
        try{
            JMVXRuntime.enter();
            junkServer = new ServerSocket(junkPort);
        } catch (IOException exception) {
            //if we run multiple experiments at once, this will fail and print
            //but for most benchmarks, it doesn't matter
            //exception.printStackTrace();
        } finally {
            JMVXRuntime.exit();
        }
    }


    // creating a DivergenceStatus object here and storing it on the instance, so that we don't
    // have to create a new one every time. We can reuse this one by setting its fields accordingly.
    DivergenceStatus<Object> divergenceStatus = new DivergenceStatus();

    protected static final boolean checksumCommunicationEnabled = JMVXRuntime.parameters.checksumCommunicationEnabled;
    protected static final boolean consoleLogsEnabled = JMVXRuntime.parameters.consoleLogsEnabled;
    protected static final boolean readsFromDisk = true; //force this to be on
    protected static final boolean circularBufferCommunicationEnabled = JMVXRuntime.parameters.circularBufferCommunicationEnabled;
    protected static final boolean avoidObjectSerialization = JMVXRuntime.parameters.avoidObjectSerialization;
    protected static final boolean useBufferBackedStreams = JMVXRuntime.parameters.useBufferBackedStreams;
    protected static final boolean deleteMmapOnDisk = JMVXRuntime.parameters.deleteMmapOnDisk;



    public void reConnect(AFUNIXSocket socket, ObjectOutputStream outputStream, ObjectInputStream inputStream, Logger l) {
        followerSocket = socket;
        toCoordinator = outputStream;
        fromCoordinator = inputStream;
        log = l;

        // Make sure that new threads use the Follower strategy
        System.setProperty(JMVXRuntime.ROLE_PROP_NAME, "Follower");
    }

    protected void sendMode() throws IOException {
        //Value is compared with lockstepMode on Coordinator
        toCoordinator.writeBoolean(false);
        toCoordinator.flush();
        toCoordinator.reset();
    }

    protected void divergence(String expected, String received) {
        log.warn("EXPECTED: " + expected + " RECEIVED: " + received + " OC: " + objectCounter);
        throw new DivergenceError();
    }

    private Object readFromBuffer() {
        return receiveObjectFromCoordinator();
        /*ObjectInput in = null;
        Object ob;
        try {
            byte[] data = buffer.readData(true);

            //TODO: Fix performance bottleneck caused by new instance creation of
            //ByteArrayInputStream and ObjectInputStream on each read operation.
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            in = new ObjectInputStream(bis){
                @Override
                protected void readStreamHeader() throws IOException, StreamCorruptedException {
                    try {
                        switch (OptimizedEvent.readTag(this)) {
                            case LENGTH:
                                return;
                            default:
                                throw new StreamCorruptedException("Invalid header");
                        }
                    }catch (IndexOutOfBoundsException e){
                        throw new StreamCorruptedException("Invalid header");
                    }
                }
            };
            ob = in.readObject();
        }catch (IOException | ClassNotFoundException e) {
            throw new Error(e);
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                // Ignore close exception
            }
        }
        return ob;*/
    }

    protected void maybeWrite(JMVXOutputStream os, int b) throws IOException {
        if (os instanceof FileOutputStream) {
            FileOutputStream fos = (FileOutputStream) os;
            FileDescriptor fd = fos.getFD();
            if (fd == FileDescriptor.out || fd == FileDescriptor.err)
                os.$JMVX$write(b);
        }
    }

    protected void maybeWrite(JMVXOutputStream os, byte[] bytes) throws IOException {
        if (os instanceof FileOutputStream) {
            FileOutputStream fos = (FileOutputStream) os;
            FileDescriptor fd = fos.getFD();
            if (consoleLogsEnabled && (fd == FileDescriptor.out || fd == FileDescriptor.err))
                os.$JMVX$write(bytes);
        }
    }

    protected void maybeWrite(JMVXOutputStream os, byte[] bytes, int off, int len) throws IOException {
        if (os instanceof FileOutputStream) {
            FileOutputStream fos = (FileOutputStream) os;
            FileDescriptor fd = fos.getFD();
            if (consoleLogsEnabled && (fd == FileDescriptor.out || fd == FileDescriptor.err))
                os.$JMVX$write(bytes, off, len);
        }
    }

    protected void verifyWriteOb(JMVXOutputStream os, int expectedB, int receivedB) throws IOException {
        if (expectedB != receivedB) {
            log.warn("Potential mismatch! Difference in byte written");
            throw new DivergenceError();
        }
        maybeWrite(os, expectedB);
    }

    protected void verifyWriteOb(JMVXRandomAccessFile raf, int expectedB, int receivedB) throws IOException {
        if (expectedB != receivedB) {
            log.warn("Potential mismatch! Difference in byte written");
            throw new DivergenceError();
        }
        //not doing the write for RandomAccess files
        //maybeWrite(raf, receivedB)
    }

    protected void verifyWriteOb(JMVXOutputStream os, byte[] expectedB, byte[] receivedB, Object write) throws IOException {
        String actualClassName;
        boolean bytesAreEqual = areBytesAreEqual(expectedB, receivedB, 0, 0, expectedB.length, receivedB.length);

        if (!bytesAreEqual) {
            log.warn("Potential mismatch! Difference in bytes written");

            Callable<Object> function = () -> {
                JMVXRuntime.write((OutputStream) os, receivedB);
                return null;
            };

            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = expectedB;
            metadata.originalBuffer = receivedB;
            metadata.isOffsetPresent = false;

            actualClassName = (write != null) ? write.getClass().getSimpleName() : "NoClassName";
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), actualClassName, write, function, metadata);
            maybeWrite(os, metadata.buffer);
        }else{
            maybeWrite(os, expectedB);
        }
    }

    protected void verifyWriteOb(JMVXRandomAccessFile raf, byte[] expectedB, byte[] receivedB, Object write) throws IOException {
        String actualClassName;
        boolean bytesAreEqual = areBytesAreEqual(expectedB, receivedB, 0, 0, expectedB.length, receivedB.length);

        if (!bytesAreEqual) {
            log.warn("Potential mismatch! Difference in bytes written");

            Callable<Object> function = () -> {
                JMVXRuntime.write((RandomAccessFile) raf, receivedB);
                return null;
            };

            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = expectedB;
            metadata.originalBuffer = receivedB;
            metadata.isOffsetPresent = false;

            actualClassName = (write != null) ? write.getClass().getSimpleName() : "NoClassName";
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), actualClassName, write, function, metadata);
            //maybeWrite(os, metadata.buffer);
        }/*else{
            maybeWrite(os, expectedB);
        }*/
    }

    /*
    Rewrite to handle better?
    This is comparing parts of 2 different byte buffers,
    but the portion of the buffer we receive should only be the pertinent part.
    In other words, we need to the offsets and lengths match,
    but the offset should be 0 for received bytes regardless.
     */
    protected void verifyWriteOb(JMVXOutputStream os, byte[] expectedBytes, byte[] recievedBytes, int expectedOff,
                                 int recievedOff, int expectedLen, int recievedLen, Object write) throws IOException {
        boolean bytesAreEqual = areBytesAreEqual(expectedBytes, recievedBytes, expectedOff, recievedOff, expectedLen, recievedLen);

        if (!bytesAreEqual) {
            String actualClassName;
            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = expectedBytes;
            metadata.originalBuffer = recievedBytes;
            metadata.originalLength = recievedLen;
            metadata.originalOffset = recievedOff;
            metadata.length = expectedLen;
            metadata.offset = expectedOff;
            metadata.isOffsetPresent = true;

            log.warn("Potential mismatch! Difference in bytes written");
            if (checksumCommunicationEnabled) {
                // if checksum communication is enabled, we can't write the leader's string to the log
                // because we don't have it
                log.warn("The leader's string checksum: " + new String(recievedBytes));
            } else {
                log.warn("The leader wrote the string: " + new String(recievedBytes, recievedOff, recievedLen));
            }
            Callable<Object> function = () -> {
                JMVXRuntime.write((OutputStream) os, expectedBytes, expectedOff, expectedLen);
                return true;
            };

            actualClassName = (write != null) ? write.getClass().getSimpleName() : "NoClassName";
            // the divergence method may modify the metadata argument in C-style pass by reference
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), actualClassName, write, function, metadata);

            log.warn("The follower is trying to write the string: " + new String(expectedBytes, expectedOff, expectedLen));
            maybeWrite(os, metadata.buffer, metadata.offset, metadata.length);
        }else{
            maybeWrite(os, expectedBytes, expectedOff, expectedLen);
        }
    }

    protected void verifyWriteOb(JMVXRandomAccessFile raf, byte[] expectedBytes, byte[] recievedBytes, int expectedOff,
                                 int recievedOff, int expectedLen, int recievedLen, Object write) throws IOException {
        boolean bytesAreEqual = areBytesAreEqual(expectedBytes, recievedBytes, expectedOff, recievedOff, expectedLen, recievedLen);

        if (!bytesAreEqual) {
            String actualClassName;
            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = expectedBytes;
            metadata.originalBuffer = recievedBytes;
            metadata.originalLength = recievedLen;
            metadata.originalOffset = recievedOff;
            metadata.length = expectedLen;
            metadata.offset = expectedOff;
            metadata.isOffsetPresent = true;

            log.warn("Potential mismatch! Difference in bytes written");
            if (checksumCommunicationEnabled) {
                // if checksum communication is enabled, we can't write the leader's string to the log
                // because we don't have it
                log.warn("The leader's string checksum: " + new String(recievedBytes));
            } else {
                log.warn("The leader wrote the string: " + new String(recievedBytes, recievedOff, recievedLen));
            }
            Callable<Object> function = () -> {
                JMVXRuntime.write((RandomAccessFile) raf, expectedBytes, expectedOff, expectedLen);
                return true;
            };

            actualClassName = (write != null) ? write.getClass().getSimpleName() : "NoClassName";
            // the divergence method may modify the metadata argument in C-style pass by reference
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), actualClassName, write, function, metadata);

            log.warn("The follower is trying to write the string: " + new String(expectedBytes, expectedOff, expectedLen));
            //maybeWrite(os, metadata.buffer, metadata.offset, metadata.length);
        }/*else{
            maybeWrite(os, expectedBytes, expectedOff, expectedLen);
        }*/
    }

    private boolean areBytesAreEqual(byte[] expectedBytes, byte[] recievedBytes, int expectedOff, int recievedOff, int expectedLen, int recievedLen) {
        if (checksumCommunicationEnabled) {
            return Arrays.equals(recievedBytes,
                    Utils.getChecksum(expectedBytes, expectedOff, expectedLen, crc32));
        } else {
            int n = Math.min(recievedLen, expectedLen);
            int i = recievedOff;
            int j = expectedOff;
            for (int k = 0; k < n; k++){
                if(recievedBytes[i+k] != expectedBytes[j+k]){
                    return false;
                }
            }
            //doesn't factor in the part of the buffer we care about
            //bytesAreEqual = Arrays.equals(recievedBytes, expectedBytes);
            return true;
        }
    }

    public static InputStream maybeBufferedInputStream(InputStream toDecorate){
        return useBufferBackedStreams ? new BufferedInputStream(toDecorate) : toDecorate;
    }

    public static OutputStream maybeBufferedOutputStream(OutputStream toDecorate){
        return useBufferBackedStreams ? new BufferedOutputStream(toDecorate) : toDecorate;
    }

    private static final int bufferSize = 10 * 1024 * 1024; //10 mb

    @Override
    public void main() {
        // Make sure ObjectOutputStream is fully initialized
        // This may involve reading file /dev/urandom
        /*try {
            new ObjectOutputStream(new ByteArrayOutputStream(8000)).writeObject(new Exception());
            skipped = new ObjectOutputStream(skippedBytes);
        } catch (IOException e) {
        }*/
        connect(1235);
        log = Logger.getLogger(Follower.class);
        log.debug("Reading directly from disk: " + readsFromDisk);

        if(circularBufferCommunicationEnabled) {
            try {
                String status = (String) fromCoordinator.readObject();
                if (status.equals("BufferInitialized")) {
                    buffer = JMVXRuntime.mmapFile(false);
                    if (deleteMmapOnDisk)
                        buffer.deleteFileOnDisk();
//                    toCoordinator.writeObject("FollowerReady");
//                    toCoordinator.flush();
//                    toCoordinator.reset();
//                    status = (String) fromCoordinator.readObject();
//                    while(!status.equals("BufferInitialized"));
//                    if (JMVXRuntime.deleteMmapOnDisk)
//                        buffer.deleteFileOnDisk();
                } else throw new Error();
            } catch (IOException | ClassNotFoundException e) {
                throw new Error(e);
            }


            try {
                fromBufferStream = new ObjectInputStream(new JMVXFastCircularBufferInputStream(buffer));
            } catch (IOException e) {
                throw new Error(e);
            }
            fromSocketStream = fromCoordinator;
            fromCoordinator = fromBufferStream;
            toSocketStream = toCoordinator;
            {
                OutputStream ops;
                AtomicBoolean flag = new AtomicBoolean(false);
                try {
                    ops = new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {
                            if (flag.get()) {
                                throw new IOException("The follower cannot write to the ringbuffer");
                            }
                        }
                    };
                    this.toCoordinator = new ObjectOutputStream(ops);
                    toBufferStream = toCoordinator;
                } catch (IOException e) {
                    throw new Error(e);
                }
                flag.set(true);
            }
        } else {
            fromSocketStream = fromCoordinator;
            fromBufferStream = fromCoordinator;
            toSocketStream = toCoordinator;
            toBufferStream = toCoordinator;
        }

        try {
            //new ObjectOutputStream(maybeBufferedOutputStream(new FileOutputStream(
            toLogFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
                    Paths.get(recordingDir, "recording." + Thread.currentThread().getName() + ".dat").toFile()), bufferSize));
            synchronized (Recorder.class) {
                streamsToFlush.put(Thread.currentThread().getName(), toLogFile);
            }

            toLogFile.writeInt(vectorClockIndex);
        }catch (IOException e){
            throw new Error("Could not make thread log file", e);
        }
        workerThread = Thread.currentThread() instanceof JMVXThreadFactory.JMVXPoolThread;
        //recorder.main(vectorClockIndex); //clock index is set in `connect` method called at the top
        //taskRef = Thread.currentThread();
    }

    private static Object clinit = new Object();

    public void connect(int port) {
        if (!connected) {
            Boolean coordinatorReady;
            /*TODO refactor to unify with Replayer's field for clockIndex
             * This will help with porting Thread Pool support later */
            int clockIndex;
            while (true) {
                try {
                    //Uncomment if switching back to inDepthClinit
                    //setupClinitSocket();

                    followerSocket = AFUNIXSocket.newInstance();
                    followerSocket.setAncillaryReceiveBufferSize(1024);
                    followerSocket.connect(Coordinator.FOLLOWER_COMMS_ADDR);
                    toCoordinator = new ObjectOutputStream(maybeBufferedOutputStream(followerSocket.getOutputStream()));

                    toCoordinator.writeObject(Thread.currentThread().getName());
                    toCoordinator.flush();

                    fromCoordinator = new ObjectInputStream(maybeBufferedInputStream(followerSocket.getInputStream()));
                    clockIndex = fromCoordinator.readInt();

                    sendMode();
                    coordinatorReady = fromCoordinator.readBoolean();
                    while (!coordinatorReady) {
                        try {
                            coordinatorReady = fromCoordinator.readBoolean();
                            Thread.sleep(100);
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }
                    break;
                } catch (IOException e) {
                    //System.out.println("Follower to Coordinator Connection Failed. Retrying...");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            }
            vectorClockIndex = JMVXRuntime.clock.registerNewThread(clockIndex);
            connected = true;

            synchronized (connections){
                connections.add(followerSocket);
            }
        }

        log = Logger.getLogger(Follower.class);
        log.info("Checksum communication enabled: " + checksumCommunicationEnabled);
    }

    /*@Deprecated
    private synchronized void setupClinitSocket() throws IOException {
        //already made the socket, skip
        if(clinitSocket == null) {
            //make the clinit Socket
            clinitSocket = AFUNIXSocket.newInstance();
            clinitSocket.connect(Coordinator.FOLLOWER_COMMS_ADDR);
            clinitOut = new ObjectOutputStream(clinitSocket.getOutputStream());

            clinitOut.writeObject("clinit");
            clinitOut.flush();

            clinitIn = new ObjectInputStream(clinitSocket.getInputStream());
        }
    }*/

    /**
     * read 1
     * never used in batik
     * @param is
     * @return
     * @throws IOException
     */
    @Override
    public int read(JMVXInputStream is) throws IOException {
        byte[] bytes = new byte[Integer.BYTES];
        if(readsFromDisk && Utils.isInputStreamFromDisk(is)){
            int ret = is.$JMVX$read();
            readRecorder.recordReadByte(toLogFile, buffer,
                    markElidedEvents ? OptimizedEvent.ELIDED_READ : OptimizedEvent.READ, ret);
            return ret;
        }
        try {
            int ret = readReplayer.replayReadByte(fromCoordinator, buffer, false);
            readRecorder.recordReadByte(toLogFile, buffer, OptimizedEvent.READ, ret);
            /* TODO: add in skip for all read methods
             * NOTE: refactored code is missing the skip that was normally here
             * The skip is important for role swap, but as that is broken for now, I'm omitting it
             * We can survive without the skip if: role swap is disabled and readsFromDisk is fully implemented for
             * the read ops in a stream. Eg we either read from disk exclusively or get events exclusively */

            //return the value read in, not the number of bytes read
            return ret;
        }catch (IOException e){
            readRecorder.logException(toLogFile, e);
            throw e;
        }catch (DivergenceError e) {
            throw e;
            /*
            Callable<Object> function = () -> {
                return JMVXRuntime.read((InputStream) is);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Read.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to read a byte");
            return (int) divergenceStatus.returnValue;
             */
        }
    }

    protected Object receiveObjectFromCoordinator() {
        try {
            objectCounter++;
            return fromCoordinator.readObject();
        }catch (EOFException e){
            return null;
        }catch (ClassNotFoundException | IOException e){
            //likely an OptionalDataException, which means the next read is primitive data, not an object!
            throw new DivergenceError(e);
        }
    }

    protected Object receiveObjectOverSocket(){
        try {
            objectCounter++;
            return fromSocketStream.readObject();
        }catch (ClassNotFoundException | IOException e){
            return null;
        }
    }

    protected void sendObjectOverSocket(Serializable o){
        try {
            //objectCounter++;
            toSocketStream.writeObject(o);
            toSocketStream.flush();
            toSocketStream.reset();
        }catch (IOException e){
            throw new DivergenceError();
        }
    }

    protected void sendObjectToLogFile(Serializable o){
        //if(loading > 0 || inClinit > 0)
        //    return;
        try {
            //objectCounter++;
            toLogFile.writeObject(o);
            toLogFile.reset();
            //no flush, we are going to disk.
        }catch (IOException e){
            throw new DivergenceError();
        }
    }
    /**
     * read 2
     * never used in batik
     * @param is
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public int read(JMVXInputStream is, byte[] bytes) throws IOException {
        if (readsFromDisk && Utils.isInputStreamFromDisk(is)) {
            //TODO: add an exception handle for this
            //it is possible for this to error, but we would miss it.
            /*should we at least read metadata here? (like WRITE_NO_VALIDATION)
             * That way, we can validate the event */
            int bytesRead = is.$JMVX$read(bytes);
            /*Leader.debug(is, bytesRead, bytes.length);

            int availableBytes = is.$JMVX$available();
            int spaceInBuffer = bytes.length - 0 - bytesRead;
            while (availableBytes > 0 && spaceInBuffer > 0) {
                bytesRead += is.$JMVX$read(bytes, bytesRead, Math.min(availableBytes, spaceInBuffer));
                availableBytes = is.$JMVX$available();
                spaceInBuffer = bytes.length - 0 - bytesRead;
            }*/

            readRecorder.recordRead(toLogFile, null,
                    markElidedEvents ? OptimizedEvent.ELIDED_READ : OptimizedEvent.READ,
                    bytes, 0, bytes.length, bytesRead);
            return bytesRead;
        } else {
            try {
                int ret = readReplayer.replayRead(fromCoordinator, buffer, false,
                        bytes, 0, bytes.length);
                readRecorder.recordRead(toLogFile, buffer, OptimizedEvent.READ,
                        bytes, 0, bytes.length, ret);
                return ret;
            } catch (IOException e) {
                readRecorder.logException(toLogFile, e);
                throw e;
            } catch (DivergenceError e) {
                throw e;
                /*
                Callable<Object> function = () -> {
                    return JMVXRuntime.read((InputStream) is, bytes);
                };
                DivergenceStatus<Object> divergenceStatus = divergence(Leader.Read.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

                if (divergenceStatus.containsException) {
                    throw (IOException) divergenceStatus.returnValue;
                }
                log.warn("The follower is trying to read the string: " + new String(bytes));
                return (int) divergenceStatus.returnValue;
                 */
            }
        }
    }

    /**
     * raed 3
     * @param is
     * @param bytes
     * @param off
     * @param len
     * @return
     * @throws IOException
     */
    @Override
    public int read(JMVXInputStream is, byte[] bytes, int off, int len) throws IOException {
        if (readsFromDisk && Utils.isInputStreamFromDisk(is)) {
            //Leader.debug(is, ret, bytes.length, off, len);
            /*int spaceInBuffer = Math.min(len, bytes.length - off);
            int bytesRead = is.$JMVX$read(bytes, off, Math.min(len, spaceInBuffer));
            spaceInBuffer -= bytesRead;
            int availableBytes = is.$JMVX$available();

            while (availableBytes > 0 && spaceInBuffer > 0) {
                bytesRead += is.$JMVX$read(bytes, bytesRead + off, Math.min(availableBytes, spaceInBuffer));
                availableBytes = is.$JMVX$available();
                spaceInBuffer -= bytesRead;
            }*/
            int bytesRead = is.$JMVX$read(bytes, off, len);
            readRecorder.recordRead(toLogFile, null,
                    markElidedEvents ? OptimizedEvent.ELIDED_READ : OptimizedEvent.READ,
                    bytes, off, len, bytesRead);
            return bytesRead;
            //log bytes
        } else {
            try {
                int ret = readReplayer.replayRead(fromCoordinator, buffer, false,
                        bytes, off, len);
                readRecorder.recordRead(toLogFile, buffer, OptimizedEvent.READ,
                        bytes, off, len, ret);
                return ret;
            } catch (IOException e) {
                readRecorder.logException(toLogFile, e);
                throw e;
            } catch (DivergenceError e) {
                throw e;
                /*Callable<Object> function = () -> {
                    return JMVXRuntime.read((InputStream) is, bytes, off, len);
                };
                Metadata metadata = new Metadata(bytes, len, off, DivergenceSource.FROM_READ);
                metadata.is = is;
                DivergenceStatus<Object> divergenceStatus = divergence(Leader.Read.class.getSimpleName(), read.getClass().getSimpleName(), read, function, metadata);

                if (divergenceStatus.containsException) {
                    throw (IOException) divergenceStatus.returnValue;
                }
                log.warn("The follower is trying to read the string: " + new String(bytes, off, len));
                return (int) divergenceStatus.returnValue;*/
            }
        }
    }
    @Override
    public int available(JMVXInputStream is) throws IOException{
        int ret = 0;
        //LeaderRecorder will drop this event
        if(readsFromDisk){
            //get return or exception
            IOException exception = null;
            try {
                ret = is.$JMVX$available();
            }catch (IOException e){
                ret = -1;
                exception = e;
            }

            //re-encode for replayer
            if(ret > -1){
                if(avoidObjectSerialization) {
                    try {
                        OptimizedEvent.writeTag(OptimizedEvent.ELIDED_AVAILABLE, toLogFile);
                        toLogFile.writeInt(Integer.BYTES); //ret, should we make this inferred for some ops?
                        toLogFile.writeInt(ret);
                        return ret;
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to write to log file", e);
                    }
                }else{
                    sendObjectToLogFile(new Leader.Available(ret));
                }
                return ret;
            }else{
                try{
                    if(avoidObjectSerialization)
                        OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, toLogFile);

                    sendObjectToLogFile(exception);
                }catch (IOException e){
                    throw new RuntimeException("Failed to write to log file", e);
                }
                throw exception;
            }
        }

        Object read = receiveObjectFromCoordinator();
        //THIS ISN'T WRITTEN DIRECTLY TO THE BUFFER?
        if (avoidObjectSerialization && read instanceof byte[]) {
            byte[] bb = (byte[]) read;
            ret =   ((bb[0] & 0xFF) << 24) |
                    ((bb[1] & 0xFF) << 16) |
                    ((bb[2] & 0xFF) << 8 ) |
                    ((bb[3] & 0xFF) << 0 );
            try {
                OptimizedEvent.writeTag(OptimizedEvent.AVAILABLE, toLogFile);
                toLogFile.writeInt(Integer.BYTES); //ret, should we make this inferred for some ops?
                toLogFile.writeInt(ret);
                return ret;
            }catch (IOException e){
                throw new RuntimeException("Failed to write to log file", e);
            }
        } else if (read instanceof Leader.Available) {
            Leader.Available a = (Leader.Available) read;
            sendObjectToLogFile(a);
            ret = a.ret;
        } else if (read instanceof IOException) {
            try{
                if(avoidObjectSerialization)
                    OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, toLogFile);

                sendObjectToLogFile((Serializable) read);
            }catch (IOException e){
                throw new RuntimeException("Failed to write to log file", e);
            }
            throw (IOException) read;
        } else {
            Callable<Object> function = () -> {
                return is.$JMVX$available();
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Read.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            Integer i = (Integer) divergenceStatus.returnValue;
            return i.intValue();
        }
        return ret;
    }

    @Override
    public void close(JMVXOutputStream os) throws IOException {
        try {
            os.$JMVX$close();
        } catch (IOException e) {
            throw e;
        }
    }

    public void flush(JMVXOutputStream os) throws IOException {
        try {
            os.$JMVX$flush();
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * write 1
     * @param os
     * @param b
     * @throws IOException
     */
    @Override
    public void write(JMVXOutputStream os, int b) throws IOException {
        if(readsFromDisk){
            if(delayedWriteRecording){
                logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_WRITE: OptimizedEvent.WRITE, b);
                maybeWrite(os, b);
            }else{
                OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
            }
            return;
        }
        Object write;
        if (circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                OptimizedEvent tag = getTagFromBuffer();
                byte[] data;
                switch (tag){
                    case WRITE:
                        data = buffer.readData(true);
                        int rb = JMVXRuntime.unsafe.getInt(data, Unsafe.ARRAY_BYTE_BASE_OFFSET);
                        verifyWriteOb(os, b, rb);
                        logWriteOperation(OptimizedEvent.WRITE, rb);
                        return;
                    case WRITE_NO_VALIDATION:
                        if(delayedWriteRecording){
                            //log our copy of the bytes
                            logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_WRITE: OptimizedEvent.WRITE, b);
                            maybeWrite(os, b);
                        }else{
                            OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
                        }
                        return;
                    case EXCEPTION:
                        write = receiveObjectFromCoordinator();
                        break; //continues to serialized data code which logs and handles the exception
                    default:
                        throw new DivergenceError();
                }
            }else {
                write = readFromBuffer();
            }
        } else {
            write = receiveObjectFromCoordinator();
        }

        sendObjectToLogFile((Serializable) write);

        if (write instanceof Leader.Write) {
            Leader.Write w = (Leader.Write) write;
            int rb = w.b;
            verifyWriteOb(os, b, rb);
        } else if (write instanceof IOException) {
            throw ((IOException) write);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.write((OutputStream) os, b);
                return true;
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), write.getClass().getSimpleName(), write, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write the character: " + (char) b);
            return;
        }
    }

    private OptimizedEvent getTagFromBuffer() {
        byte[] data = buffer.readData(true);
        return OptimizedEvent.fromByte(data[0]);
    }

    protected DivergenceStatus<Object> divergence(String expectedClassName, String actualClassName, Object message, Callable<Object> function) {
        return divergence(expectedClassName, actualClassName, message, function, null);
    }

    private DivergenceStatus<Object> divergence(String expectedClassName, String actualClassName, Object message, Callable<Object> function, Metadata metadata) {
        divergenceStatus.hasDivergenceOccured = true; // explicity setting this field to true here
        // we assume beforehand that things are not going to go well

        if (message instanceof SwitchRolesEvent) {
            Leader strategy = new Leader();
            // set this to something larger than 1 to avoid entering the promote/demote strategy again.
            Leader.one.set(20);
            strategy.reConnect(followerSocket, toCoordinator, fromCoordinator, log);
            JMVXRuntime.setStrategy(ReentrantStrategy.singleLeaderPassthrough(strategy));
            JMVXRuntime.openExistingFiles();

            try { // case 1
                // we recieved a SwitchRolesEvent, hence we call the lambda to run any pending code
                // stuff the return value of the lambda in the divergenceStatus
                // set the hasDivergenceOccured flag to false and return
                Object o = function.call();
                divergenceStatus.returnValue = o;
                divergenceStatus.hasDivergenceOccured = false;
                divergenceStatus.containsException = false;
            } catch (Exception e) {
                // case 2
                // we recieved a SwitchRolesEvent, hence we call the lambda to run any pending code
                // but the lambda threw an exception
                // we stuff the exception that occured in the placeholder for the return value
                // set the containsException flag to true and
                // set the hasDivergenceOccured flag to false and return
                divergenceStatus.hasDivergenceOccured = false;
                divergenceStatus.returnValue = e;
                divergenceStatus.containsException = true;
            }
            return divergenceStatus;
        } else { // case 3
            // now we know that a divergence has occured because
            // we got an object from the coordinator that we didn't expect
            // so we call our custom divergence handler
            // and send back to the caller whatever return value the divergence handler
            // returned to us
            // Lastly, set the hasDivergenceOccured flag to true and return
            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
            List<StackTraceElement> stack = Arrays.asList(stackTraceElements);
            HandlerStatus r = null;
            List<StackTraceElement> filteredStack = stack.stream().filter(element -> !element.getClass().getName().contains("jmvx")).collect(Collectors.toList());

            if (metadata == null) {
                r = divergenceHandler.handleDivergence(filteredStack);
            } else {
                switch (metadata.source) {
                    case FROM_READ: {
                        r = divergenceHandler.handleReadDivergence(filteredStack, metadata);
                    } break;
                    case FROM_WRITE: {
                        r = divergenceHandler.handleWriteDivergence(filteredStack, metadata);
                    }
                }
            }

            if (r.status == DivergenceStatus.STATUS.NOT_OK) {
                // terminating the program if the divergence handler returns NOT_OK
                throw new DivergenceError();
            }

            divergenceStatus.hasDivergenceOccured = true;
            divergenceStatus.returnValue = r.returnValue;
            divergenceStatus.containsException = false;
            log.warn("divergence occured");
            return divergenceStatus;
        }
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes) throws IOException {
        if(readsFromDisk){
            if(delayedWriteRecording){
                logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_WRITE: OptimizedEvent.WRITE, bytes);
                maybeWrite(os, bytes);
            }else{
                OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
            }
            return;
        }
        Object write;
        if(circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                OptimizedEvent tag = getTagFromBuffer();
                switch (tag){
                    case WRITE:
                        byte[] receivedBytes = buffer.readData(true);
                        verifyWriteOb(os, bytes, receivedBytes, null);
                        logWriteOperation(OptimizedEvent.WRITE, receivedBytes);
                        return;
                    case WRITE_NO_VALIDATION:
                        if(delayedWriteRecording){
                            logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_WRITE: OptimizedEvent.WRITE, bytes);
                            maybeWrite(os, bytes);
                        }else{
                            OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
                        }
                        return;
                    case EXCEPTION:
                        write = receiveObjectFromCoordinator();
                        break;
                    default:
                        throw new DivergenceError();
                }
            } else {
                write = readFromBuffer();
            }
        } else {
            write = receiveObjectFromCoordinator();
        }

        sendObjectToLogFile((Serializable) write);

        if (write instanceof Leader.WriteB) {
            Leader.WriteB w = (Leader.WriteB) write;
            byte[] b = w.b;
            verifyWriteOb(os, bytes, b, write);
        } else if (write instanceof IOException) {
            throw ((IOException) write);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.write((OutputStream) os, bytes);
                return true;
            };
            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = bytes;
            metadata.isOffsetPresent = false;
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.WriteB.class.getSimpleName(), write.getClass().getSimpleName(), write, function, metadata);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write the string: " + new String(bytes));
            return;
        }
    }

    /**
     * write 3
     * @param os
     * @param bytes
     * @param off
     * @param len
     * @throws IOException
     */
    @Override
    public void write(JMVXOutputStream os, byte[] bytes, int off, int len) throws IOException {
        if(readsFromDisk){
            if(delayedWriteRecording){
                logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_WRITE: OptimizedEvent.WRITE,
                        bytes, off, len);
                maybeWrite(os, bytes, off, len);
            }else{
                OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
            }
            return;
        }
        Object write;
        if(circularBufferCommunicationEnabled) {
            if(avoidObjectSerialization) {
                OptimizedEvent tag = getTagFromBuffer();
                switch(tag){
                    case WRITE:
                        int byteOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
                        byte[] data = buffer.readData(true);
                        long byteLen = JMVXRuntime.unsafe.getLong(data, byteOffset);
                        byte[] receivedB =  new byte[(int)byteLen];
                        long otherFieldsStart = byteOffset + Long.BYTES + byteLen;

                        JMVXRuntime.unsafe.copyMemory(data, byteOffset + Long.BYTES, receivedB, byteOffset, byteLen);
                        int receivedOff = JMVXRuntime.unsafe.getInt(data, otherFieldsStart);
                        int receivedLen = JMVXRuntime.unsafe.getInt(data, otherFieldsStart + Integer.BYTES);

                        verifyWriteOb(os, bytes, receivedB, off, receivedOff, len, receivedLen, null);
                        logWriteOperation(OptimizedEvent.WRITE, bytes, receivedOff, receivedLen);
                        return;
                    case WRITE_NO_VALIDATION:
                        if(delayedWriteRecording){
                            //log our copy of the data
                            logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_WRITE: OptimizedEvent.WRITE, bytes, off, len);
                            maybeWrite(os, bytes, off, len);
                        }else{
                            OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
                        }
                        return;
                    case EXCEPTION:
                        write = receiveObjectFromCoordinator(); //fallthrough to log to file and handle
                        break;
                    default:
                        throw new DivergenceError();
                }
            } else {
                write = readFromBuffer();
            }
        } else {
            write = receiveObjectFromCoordinator();
        }

        sendObjectToLogFile((Serializable) write);

        if (write instanceof Leader.WriteBII) {
            Leader.WriteBII w = (Leader.WriteBII) write;
            verifyWriteOb(os, bytes, w.b, off, w.off, len, w.len, write);
        } else if (write instanceof IOException) {
            throw ((IOException) write);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.write((OutputStream) os, bytes, off, len);
                return true;
            };
            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = bytes;
            metadata.length = len;
            metadata.offset = off;
            metadata.isOffsetPresent = false;
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), write.getClass().getSimpleName(), write, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write the string: " + new String(bytes, off, len));
        }
    }

    private void logWriteOperation(OptimizedEvent writeTag, byte[] bytes, int offset, int len) {
        try{
            OptimizedEvent.writeTag(writeTag, toLogFile);
            //should we check if len >= 0?
            toLogFile.writeInt(
                    (Integer.BYTES * 2) //offset, len
                            + len //space for bytes
            );
            toLogFile.writeInt(offset);
            toLogFile.writeInt(len);
            toLogFile.write(bytes, offset, len);
        }catch (IOException e){
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    private void logWriteOperation(OptimizedEvent writeTag, byte[] bytes) {
        try{
            int len = bytes.length;
            OptimizedEvent.writeTag(writeTag, toLogFile);
            toLogFile.writeInt(
                    len //space for bytes
            );
            toLogFile.write(bytes, 0, len);
        }catch (IOException e){
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    private void logWriteOperation(OptimizedEvent writeTag, int b) {
        try{
            OptimizedEvent.writeTag(writeTag, toLogFile);
            toLogFile.writeInt(
                    (Integer.BYTES) //len
            );
            toLogFile.writeInt(b);
        }catch (IOException e){
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    @Override
    public boolean canRead(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileCanRead) {
            ret = ((Leader.FileCanRead) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.canRead((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileCanRead.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to check if the file: " + f.getClass().getSimpleName() + " can be read");

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    /**
     * never reached in batik
     * @param f
     * @return
     */
    @Override
    public boolean canWrite(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileCanWrite) {
            ret = ((Leader.FileCanWrite) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.canWrite((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileCanWrite.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write to the file: " + f.getClass().getSimpleName());

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public boolean createNewFile(JMVXFile f) throws IOException {
        /*should we block on this method? That way other calls, like canRead/canWrite/stat, can work without comms? */
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileCreateNewFile) {
            ret = ((Leader.FileCreateNewFile) file).ret;
        } else if (file instanceof IOException) {
            throw ((IOException) file);
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.createNewFile((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileCreateNewFile.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to create file: " + f.getClass().getSimpleName());

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public boolean delete(JMVXFile f) {
        if(loading > 0 || inClinit > 0){
            throw new UnsupportedOperationException("Not implemented yet -- logic has to differ, we can't send an ACK");
        }
        boolean ret = false;
        Object file = receiveObjectOverSocket();
        if (file instanceof Leader.FileDelete) {
            ret = ((Leader.FileDelete) file).ret;
            String status = (String) receiveObjectOverSocket();
            if (status.equals("waiting")) {
                sendObjectOverSocket("synced");
                ret = (boolean) receiveObjectOverSocket();
                sendObjectToLogFile(new Leader.FileDelete(ret));
            }else{
                throw new DivergenceError();
            }
        } else{
            Callable<Object> function = () -> {
                return JMVXRuntime.delete((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileDelete.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to delete file: " + f.getClass().getSimpleName());
            return (boolean) divergenceStatus.returnValue;
        }
        return ret;
    }

    @Override
    public boolean exists(JMVXFile f) {
        if(loading > 0) {
            return redirect(f).$JMVX$exists();
        }

        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileExists) {
            ret = ((Leader.FileExists) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.exists((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileExists.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to check if file: " + f.getClass().getSimpleName() + " can be read");

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    /**
     * never used in batik
     * @param f
     * @return
     */
    @Override
    public File getAbsoluteFile(JMVXFile f) {
        File ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileGetAbsoluteFile) {
            ret = ((Leader.FileGetAbsoluteFile) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getAbsoluteFile((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetAbsoluteFile.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the canonical file: " + f.getClass().getSimpleName());

            return (File)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public String getAbsolutePath(JMVXFile f) {
        String ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileGetAbsolutePath) {
            ret = ((Leader.FileGetAbsolutePath) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getAbsolutePath((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetAbsolutePath.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the canonical file: " + f.getClass().getSimpleName());

            return (String)divergenceStatus.returnValue;
        }

        return ret;
    }

    /**
     * never used in batik
     * @param f
     * @return
     * @throws IOException
     */
    @Override
    public File getCanonicalFile(JMVXFile f) throws IOException {
        File ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileGetCanonicalFile) {
            ret = ((Leader.FileGetCanonicalFile) file).ret;
        } else if (file instanceof IOException) {
            throw ((IOException) file);
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getCanonicalFile((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetCanonicalFile.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the canonical file: " + f.getClass().getSimpleName());

            return (File)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public String getCanonicalPath(JMVXFile f) throws IOException {
        String ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileGetCanonicalPath) {
            ret = ((Leader.FileGetCanonicalPath) file).ret;
        } else if (file instanceof IOException) {
            throw ((IOException) file);
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getCanonicalPath((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetCanonicalPath.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the parent file of file: " + f.getClass().getSimpleName());

            return (String)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public String getName(JMVXFile f) {
        String ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileGetName) {
            ret = ((Leader.FileGetName) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getName((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetName.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the parent file of file: " + f.getClass().getSimpleName());

            return (String)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public File getParentFile(JMVXFile f) {
        File ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileGetParentFile) {
            ret = ((Leader.FileGetParentFile) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getParentFile((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetParentFile.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the parent file of file: " + f.getClass().getSimpleName());

            return (File)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public String getPath(JMVXFile f) {
        throw new UnsupportedOperationException("Shouldn't get here");
        /*
        String ret = null;
        Object file = receiveObjectFromCoordinator();
        if (file instanceof Leader.FileGetPath) {
            ret = ((Leader.FileGetPath) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.getPath((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileGetPath.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the directory of file: " + f.getClass().getSimpleName());

            return (String)divergenceStatus.returnValue;
        }

        return ret;*/
    }

    @Override
    public boolean isDirectory(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileIsDirectory) {
            ret = ((Leader.FileIsDirectory) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.isDirectory((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileIsDirectory.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the directory of file: " + f.getClass().getSimpleName());

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    /**
     * never called in batik
     * @param f
     * @return
     */
    @Override
    public long length(JMVXFile f) {
        long ret = 0;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileLength) {
            ret = ((Leader.FileLength) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.length((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileLength.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the length of file: " + f.getClass().getSimpleName());

            return (long)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public String[] list(JMVXFile f) {
        String[] ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileList) {
            ret = ((Leader.FileList) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.list((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileList.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the list of filename: " + f.getClass().getSimpleName());

            return (String[])divergenceStatus.returnValue;
        }

        return ret;
    }

    public String[] list(JMVXFile f, FilenameFilter filter) {
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileListFilter) {
            String[] unfiltered = ((Leader.FileListFilter) file).unfiltered;
            if (unfiltered == null)
                return null;
            return Leader.filterFiles(unfiltered, filter);
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.list((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileList.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the list of filename: " + f.getClass().getSimpleName());

            return (String[])divergenceStatus.returnValue;
        }
    }

    @Override
    public File[] listFiles(JMVXFile f) {
        File[] ret = null;
        Object file = circularBufferCommunicationEnabled ? readFromBuffer() : receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileListFiles) {
            ret = ((Leader.FileListFiles) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.listFiles((File) f);
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileListFiles.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to list files: " + f.getClass().getSimpleName());

            return (File[])divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public File[] listFiles(JMVXFile f, FilenameFilter filter) {
        Object read = circularBufferCommunicationEnabled ? readFromBuffer() : receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.FileListFilesFilter){
            //Have to run the filter over the entire listing of files to capture the results of side effects
            File[] unfiltered = ((Leader.FileListFilesFilter)read).unfiltered;
            return Leader.filterFiles(unfiltered, filter);
        }else if(read instanceof SecurityException){
            throw (SecurityException) read;
        }else{
            Callable<Object> function = () -> f.$JMVX$listFiles(filter);
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileListFilesFilter.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            return (File[])divergenceStatus.returnValue;
        }
    }

    @Override
    public boolean mkdir(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileMkdir) {
            ret = ((Leader.FileMkdir) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.mkdir((File) f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileMkdir.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }

            log.warn("The follower is trying to make dir " + f.getClass().getSimpleName());

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public boolean mkdirs(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileMkdirs) {
            ret = ((Leader.FileMkdirs) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.mkdirs((File)f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileMkdirs.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            log.warn("The follower is trying to make the directory path: " + f.$JMVX$getAbsolutePath());

            return (boolean)divergenceStatus.returnValue;

        }

        return ret;
    }

    /**
     * never used in batik
     * @param f
     * @param dest
     * @return
     */
    @Override
    public boolean renameTo(JMVXFile f, File dest) {
        boolean ret = false;
        File d = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileRenameTo) {
            ret = ((Leader.FileRenameTo) file).ret;
            d = ((Leader.FileRenameTo) file).f;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.renameTo((File) f, dest);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileRenameTo.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }

            log.warn("The follower is trying to rename file " + f.getClass().getSimpleName() + " to " + dest.getName());

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    /**
     * never used in batik
     * @param f
     * @return
     */
    @Override
    public boolean setReadOnly(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileSetReadOnly) {
            ret = ((Leader.FileSetReadOnly) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.setReadOnly((File) f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileSetReadOnly.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }

            log.warn("The follower is trying to set read only on file " + f.getClass().getSimpleName());

            return (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public URL toURL(JMVXFile f) throws MalformedURLException {
        URL ret = null;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileToURL) {
            ret = ((Leader.FileToURL) file).ret;
        } else if (file instanceof MalformedURLException) {
            throw ((MalformedURLException) file);
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.toURL((File) f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileToURL.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (MalformedURLException)divergenceStatus.returnValue;
            }

            log.warn("The follower is trying to convert to a URL file " + f.getClass().getSimpleName());

            return (URL)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public long lastModified(JMVXFile f) {
        if(loading > 0) {
            return redirect(f).$JMVX$lastModified();
        }

        long ret = 0;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if (file instanceof Leader.FileLastModified) {
            ret = ((Leader.FileLastModified) file).ret;
        } else {
            Callable<Object> function = () -> {
                return JMVXRuntime.lastModified((File) f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileLastModified.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get last modified date of file" + f.$JMVX$getName());

            return (long)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public boolean isFile(JMVXFile f) {
        boolean ret = false;
        Object file = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) file);
        if(file instanceof Leader.IsFile) {
            ret = ((Leader.IsFile) file).ret;
        }else if(file instanceof SecurityException){
            throw (SecurityException)file;

        }else{
            Callable<Object> function = () -> {
                return JMVXRuntime.isFile((File) f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.IsFile.class.getSimpleName(), file.getClass().getSimpleName(), file, function);

            if(divergenceStatus.containsException){
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to see if this is a valid file:" + f.$JMVX$getName());

            ret = (boolean)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public void open(JMVXFileOutputStream fos, String name, boolean append) throws FileNotFoundException {
        Object fileOS;
        fileOS = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) fileOS);

        if (fileOS instanceof Leader.FileOutputStreamOpen) {
            Leader.FileOutputStreamOpen foso = (Leader.FileOutputStreamOpen) fileOS;
            String n = foso.name;
            boolean a = foso.append;
            if (!n.equals(name) || a != append) {
                log.warn("Potential mismatch! Difference in files opened");
                log.warn("The leader tried to open: " + n + " " + a);
                log.warn("The follower is trying to open: " + name + " " + append);
            }
            fos.$JMVX$open("/dev/null", false);
            JMVXRuntime.openedFiles.put(fos, name);
        } else if (fileOS instanceof FileNotFoundException) {
            throw ((FileNotFoundException) fileOS);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.open((FileOutputStream) fos, name, append);
                return null; // returning null because there's nothing to return for open - it is void
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileOutputStreamOpen.class.getSimpleName(), fileOS.getClass().getSimpleName(), fileOS, function);

            // if the divergenceStatus contains an exception, we
            // throw the exception back to the caller
            // casting it to FileNotFoundException for now but that's risky!
            if (divergenceStatus.containsException) {
                throw (FileNotFoundException)divergenceStatus.returnValue;
            }
            // in the case that a divergence occured, we log that a divergence has occured
            // and return the returnValue in divergenceStatus
            // However, if a divergence has occured, the returnValue will contain the return value the divergence handler
            // otherwise the returnValue will contain the return value of the lambda
            // In either case we simply return because open() returns void
            log.warn("The follower is trying to open file " + fos.getClass().getSimpleName());
            return;
            // in this case we have just a return because the function is void
//                Consumer<Void> function = (doNotUse) -> {
//                    try {
//                        JMVXRuntime.open((FileOutputStream) fos, name, append);
//                    }  catch (FileNotFoundException e) {
//                        e.printStackTrace();
//                    }
//                };
//                boolean isDiverged = divergence(Leader.Write.class.getSimpleName(), fileOS.getClass().getSimpleName(), fileOS, function);
//                if (isDiverged) {
//                    log.warn("The follower is trying to set read only on file " + fos.getClass().getSimpleName());
//                }
//                return;
        }
    }

    @Override
    public void open(JMVXFileInputStream fis, String name) throws FileNotFoundException {
        Object fileIS;
        fileIS = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) fileIS);

        if (fileIS instanceof Leader.FileInputStreamOpen) {
            Leader.FileInputStreamOpen fiso = (Leader.FileInputStreamOpen) fileIS;
            String n = fiso.name;
            if (!n.equals(name)) {
                log.warn("Potential mismatch! Difference in files opened");
                log.warn("The leader tried to open: " + n);
                log.warn("The follower is trying to open: " + name);
            }
            fis.$JMVX$open(name);
        } else if (fileIS instanceof FileNotFoundException) {
            throw ((FileNotFoundException) fileIS);
        } else {

            Callable<Object> function = () -> {
                JMVXRuntime.open((FileInputStream) fis, name);
                return null;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileInputStreamOpen.class.getSimpleName(), fileIS.getClass().getSimpleName(), fileIS, function);

            if (divergenceStatus.containsException) {
                throw (FileNotFoundException)divergenceStatus.returnValue;
            }
        }
    }

    @Override
    public String fileOutputStream(String name) {
        return "/dev/null";
    }

    @Override
    public boolean fileOutputStream(boolean append) {
        return false;
    }

    @Override
    public File fileOutputStream(File file) {
        return new File("/dev/null");
    }

    @Override
    public FileDescriptor fileOutputStream(FileDescriptor fdObj) {
        //TODO: Figure out how to handle properly
        FileOutputStream fos;
        try {
            fos = new FileOutputStream("/dev/null");
            return fos.getFD();
        } catch (IOException ignored) {
        }
        return fdObj;
    }

    @Override
    public void sync(FileDescriptor fd) throws SyncFailedException {
        Object obj = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) obj);
        if (obj instanceof Leader.FileDescriptorSync) {
            return;
        } else if (obj instanceof SyncFailedException) { //throw an exception if we are given one
            throw (SyncFailedException) obj;
        } else {
            Callable<Object> function = () -> {
                fd.sync();
                return null;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileDescriptorSync.class.getSimpleName(), obj.getClass().getSimpleName(), obj, function);

            if (divergenceStatus.containsException) {
                throw (SyncFailedException) divergenceStatus.returnValue;
            }
        }
    }

    public void connect(JMVXSocket sock, SocketAddress endpoint, int timeout) throws IOException {
        Object obj = receiveObjectOverSocket();
        sendObjectToLogFile((Serializable) obj);
        if (obj instanceof Leader.Connect) {
            Leader.Connect cnt = (Leader.Connect) obj;
            //TODO, check if we are trying to connect to the junk socket and then skip some ops?
            //check for divergences
            if (!cnt.endpoint.equals(endpoint) || cnt.timeout != timeout) {
                log.warn("Potential mismatch! Difference in socket connect operation");
                log.warn("Leader wants to connect to " + cnt.endpoint.toString() + " with timeout of " + Integer.toString(cnt.timeout));
                log.warn("Follower wants to connect to " + endpoint.toString() + " with timeout of " + Integer.toString(timeout));
            }

            try {
                //simulate the connection via a loopback!
                synchronized (junkServer) {
                    sock.$JMVX$connect(junkAddress, 0);
                    //sets up internal state of the socket
                    Socket accepted = junkServer.accept();
                    //switch the file descriptor the socket uses
                    FileDescriptor[] fds = followerSocket.getReceivedFileDescriptors();
                    FileDescriptorUtils.swapFd((Socket) sock, fds[0], true);
                    //close the old connection used by the loopback
                    accepted.close();
                }
            } catch (Exception e) {
                log.error("Error connecting to junk server");
                e.printStackTrace();
            }
            //FileDescriptorUtils.setPort((Socket) sock, ((InetSocketAddress)endpoint).getPort());
        } else if (obj instanceof IOException) { //throw an exception if we are given one
            //TODO implement more exceptions (connect can throw more than IOException)
            throw (IOException) obj;
        }else{
            Callable<Object> function = () -> {
                JMVXRuntime.connect((Socket)sock, endpoint, timeout);
                return true;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Connect.class.getSimpleName(), obj.getClass().getSimpleName(), obj, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
        }
    }

    @Override
    public void bind(JMVXServerSocket serv, SocketAddress endpoint, int backlog) throws IOException {
        Object obj = receiveObjectOverSocket();
        sendObjectToLogFile((Serializable) obj);
        if(obj instanceof Leader.Bind){
            Leader.Bind bind = (Leader.Bind)obj;

            //check for divergences
            if(!bind.endpoint.equals(endpoint) || bind.backlog != backlog){
                log.warn("Potential mismatch! Difference in ServerSocket bind operation");
                log.warn("Leader wants to bind to " + bind.endpoint.toString() + " with backlog of " + Integer.toString(bind.backlog));
                log.warn("Follower wants to bind to " + endpoint.toString() + " with backlog of " + Integer.toString(backlog));
            }
            //null means any open port
            serv.$JMVX$bind(null, backlog);

            //switch the file descriptor the ServerSocket uses
            FileDescriptor[] fds = followerSocket.getReceivedFileDescriptors();
            try {
                FileDescriptorUtils.swapFd((ServerSocket) serv, fds[0], true);
                FileDescriptorUtils.setPort((ServerSocket) serv, ((InetSocketAddress) endpoint).getPort());
            }catch (Exception e){
                e.printStackTrace();
            }
        }else if(obj instanceof IOException){ //throw an exception if we are given one
            //TODO implement more exceptions (connect can throw more than IOException)
            throw (IOException) obj;
        }else{
            Callable<Object> function = () -> {
                JMVXRuntime.bind((ServerSocket)serv, endpoint, backlog);
                return true;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Bind.class.getSimpleName(), obj.getClass().getSimpleName(), obj, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
        }
    }

    @Override
    public Socket accept(JMVXServerSocket serv) throws IOException {
        Object obj = receiveObjectOverSocket();
        sendObjectToLogFile((Serializable) obj);
        if(obj instanceof Leader.Accept){
            //don't actually need the object
            Leader.Accept acc = (Leader.Accept)obj;

            //create a new socket and swap its file descriptor
            synchronized (junkServer){
                new Socket(junkAddress.getHostName(), junkAddress.getPort());
                Socket sock = junkServer.accept();
                FileDescriptor[] fds = followerSocket.getReceivedFileDescriptors();
                try {
                    FileDescriptorUtils.swapFd(sock, fds[0], true);
                }catch (Exception e){
                    e.printStackTrace();
                }
                return sock;
            }
            //FileDescriptorUtils.setPort(sock, acc.localPort);
        }else if(obj instanceof IOException){ //throw an exception if we are given one
            throw (IOException) obj;
        }else{
            Callable<Object> function = () -> {
                return JMVXRuntime.accept((ServerSocket) serv);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Accept.class.getSimpleName(), obj.getClass().getSimpleName(), obj, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException) divergenceStatus.returnValue;
            }

            return (Socket) divergenceStatus.returnValue;
        }
    }

    public void monitorenter(Object o) {
//        if (JMVXRuntime.clock.size() == 1) {
//            JMVXRuntime.unsafe.monitorEnter(o);
//            return;
//        }
        /* batik triggers this
         * How do we want to handle? If we adjust the vector clock, we may fail to sync on the leader
         * At the same time, we need to log the operation for the replayer...
         */
        /* Uncertain which thread initializes a class, for skip it */
        if (inClinit > 0 || loading > 0) {
            JMVXRuntime.unsafe.monitorEnter(o);
            return;
        }

        Object read = null;
        long[] clockCopy = null;
        OptimizedEvent tag = OptimizedEvent.LENGTH; //placeholder, will lead to a divergence if not set
        /* Only one of these three branches will ever execute.
         * I hate the copy pasted code to decode data, but its getting rid of the need
         * to use extra branches in a region where we really want to go fast
         * Plus decoding here allows me to figure out the new encoding for the log file
         */
        if(circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                //throw new UnsupportedOperationException("Not Implemented");
                //buffer is [tag, clock bytes...]
                byte[] bufferBytes = buffer.readData(true);
                tag = OptimizedEvent.fromByte(bufferBytes[0]);
                int clockSizeBytes = bufferBytes.length - OptimizedEvent.TAG_SIZE;
                clockCopy = new long[clockSizeBytes/Long.BYTES];

                JMVXRuntime.unsafe.copyMemory(bufferBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + OptimizedEvent.TAG_SIZE,
                        clockCopy, Unsafe.ARRAY_LONG_BASE_OFFSET,
                        bufferBytes.length);

                try {
                    //"remux" the bytes. Trying not to re-encode them from a long[], but use the raw bytes we read (for speed)
                    switch (tag) {
                        case MONITOR_ENTER:
                            //from Recorder.writeClockSlowPath
                            OptimizedEvent.writeTag(tag, toLogFile);
                            /*
                            Size of the clock is inferred based on this
                            Less expensive to compute the size than perform more IO
                             */
                            toLogFile.writeInt(
                                    clockSizeBytes //clock itself
                            );
                            toLogFile.write(bufferBytes, OptimizedEvent.TAG_SIZE, clockSizeBytes);
                            break;
                        case FAST_MONITOR_ENTER:
                            //from Recorder.writeClockFastPath
                            OptimizedEvent.writeTag(tag, toLogFile);
                            break;
                    }
                }catch (IOException e){
                    //failed to write to log file
                    throw new DivergenceError();
                }
            }else {
                read = readFromBuffer();
                sendObjectToLogFile((Serializable) read);
                if(read instanceof Leader.MonitorEnter) {
                    Leader.MonitorEnter enter = (Leader.MonitorEnter) read;
                    tag = OptimizedEvent.fromByte(enter.tagByte);
                    clockCopy = enter.clock;
                } else {
                    Callable<Object> function = () -> {
                        JMVXRuntime.monitorenter(o);
                        return null;
                    };

                    DivergenceStatus<Object> divergenceStatus = divergence(Leader.MonitorEnter.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

                    if (divergenceStatus.containsException) {
                        throw new DivergenceError();
                    }
                }
            }
        }else{
            read = readFromBuffer();
            sendObjectToLogFile((Serializable) read);
            if(read instanceof Leader.MonitorEnter) {
                Leader.MonitorEnter enter = (Leader.MonitorEnter) read;
                tag = OptimizedEvent.fromByte(enter.tagByte);
                clockCopy = enter.clock;
            } else {
                Callable<Object> function = () -> {
                    JMVXRuntime.monitorenter(o);
                    return null;
                };

                DivergenceStatus<Object> divergenceStatus = divergence(Leader.MonitorEnter.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

                if (divergenceStatus.containsException) {
                    throw new DivergenceError();
                }
            }
        }

        switch (tag){
            case SINGLE_THREADED: //avoid vc
                JMVXRuntime.unsafe.monitorEnter(o);
                return;
            case MONITOR_ENTER: //have to sync
                JMVXRuntime.clock.sync(vectorClockIndex, clockCopy, true);
                //fall through to grab lock and increment
            case FAST_MONITOR_ENTER: //avoid sync, but update vc
                JMVXRuntime.unsafe.monitorEnter(o);
                JMVXRuntime.clock.increment(vectorClockIndex, null);
                break;
            default: //divergence, only happens if we screw up somewhere
                throw new DivergenceError();
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
            // Just get the lock back
            o.wait(timeout, nanos);
            return;
        }

        int i = 0;
        while(Thread.holdsLock(o)) {
            i += 1;
            JMVXRuntime.unsafe.monitorExit(o);
        }

        Object w = null;
        long[] clockCopy = null;
        OptimizedEvent tag = OptimizedEvent.LENGTH; //placeholder, will lead to a divergence if not set
        /* Only one of these three branches will ever execute.
         * I hate the copy pasted code to decode data, but its getting rid of the need
         * to use extra branches in a region where we really want to go fast
         * Plus decoding here allows me to figure out the new encoding for the log file
         */
        if(circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                //throw new UnsupportedOperationException("Not Implemented");
                //buffer is [tag, clock bytes...]
                byte[] bufferBytes = buffer.readData(true);
                tag = OptimizedEvent.fromByte(bufferBytes[0]);
                int clockSizeBytes = bufferBytes.length - OptimizedEvent.TAG_SIZE;
                clockCopy = new long[clockSizeBytes/Long.BYTES];

                JMVXRuntime.unsafe.copyMemory(bufferBytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + OptimizedEvent.TAG_SIZE,
                        clockCopy, Unsafe.ARRAY_LONG_BASE_OFFSET,
                        bufferBytes.length);

                try {
                    //"remux" the bytes. Trying not to re-encode them from a long[], but use the raw bytes we read (for speed)
                    switch (tag) {
                        case WAIT:
                            //from Recorder.writeClockSlowPath
                            OptimizedEvent.writeTag(tag, toLogFile);
                            /*
                            Size of the clock is inferred based on this
                            Less expensive to compute the size than perform more IO
                             */
                            toLogFile.writeInt(
                                    clockSizeBytes //clock itself
                            );
                            toLogFile.write(bufferBytes, OptimizedEvent.TAG_SIZE, clockSizeBytes);
                            break;
                        case FAST_WAIT:
                            //from Recorder.writeClockFastPath
                            OptimizedEvent.writeTag(tag, toLogFile);
                            break;
                    }
                }catch (IOException e){
                    //failed to write to log file
                    throw new DivergenceError();
                }
            }else {
                w = readFromBuffer();
                sendObjectToLogFile((Serializable) w);
                if(w instanceof Leader.Wait) {
                    Leader.Wait wait = (Leader.Wait) w;
                    tag = OptimizedEvent.fromByte(wait.tagByte);
                    clockCopy = wait.clock;
                } else {
                    Callable<Object> function = () -> {
                        JMVXRuntime.wait((Object) o, timeout, nanos);
                        return null;
                    };

                    DivergenceStatus<Object> divergenceStatus = divergence(Leader.Wait.class.getSimpleName(), w.getClass().getSimpleName(), w, function);

                    if (divergenceStatus.containsException) {
                        throw (InterruptedException) divergenceStatus.returnValue;
                    }
                }
            }
        }else{
            w = receiveObjectFromCoordinator();
            sendObjectToLogFile((Serializable) w);
            if(w instanceof Leader.Wait) {
                Leader.Wait wait = (Leader.Wait) w;
                tag = OptimizedEvent.fromByte(wait.tagByte);
                clockCopy = wait.clock;
            } else {
                Callable<Object> function = () -> {
                    JMVXRuntime.wait((Object) o, timeout, nanos);
                    return null;
                };

                DivergenceStatus<Object> divergenceStatus = divergence(Leader.Wait.class.getSimpleName(), w.getClass().getSimpleName(), w, function);

                if (divergenceStatus.containsException) {
                    throw (InterruptedException) divergenceStatus.returnValue;
                }
            }
        }

        // Wait for our turn to get the lock
        switch (tag) {
            case SINGLE_THREADED: //avoid vc
                // Get the lock
                for(; i != 0; i--)
                    JMVXRuntime.unsafe.monitorEnter(o);
                return;
            case WAIT: //have to sync
                JMVXRuntime.clock.sync(vectorClockIndex, clockCopy, true);
                //fall through to grab lock and increment
            case FAST_WAIT: //avoid sync, but update vc
                // Get the lock
                for(; i != 0; i--)
                    JMVXRuntime.unsafe.monitorEnter(o);
                JMVXRuntime.clock.increment(vectorClockIndex, null);
                break;
            default: //divergence
                throw new DivergenceError();
        }
    }

    @Override
    public void threadStart(Runnable r) {

    }

    @Override
    public void threadExit(Runnable r) {
        exitLogic();
    }

    @Override
    public void exitLogic() {
        sendObjectOverSocket(CoordinatorThread.exitMessage); //Exit
        if (Thread.currentThread().getName().equals(JMVXRuntime.mainThread.getName()))
            flushData(true);
    }

    /*boolean to indicate we've started flushing data at the end of execution */
    private static volatile boolean flushing = false;

    /**
     * Flushes streams and copies jar files
     */
    private static void flushData(boolean closeFiles) {
        try {
            //these operations should not be logged!
            flushing = true;
            JMVXRuntime.enter(); //does this need to be locked?
            for (Flushable f : streamsToFlush.values()) {
                try {
                    f.flush();
                } catch (IOException e) {
                    continue;
                }
            }

            /*try (ObjectOutputStream jarPaths = new ObjectOutputStream(new BufferedOutputStream(
                    new FileOutputStream(Paths.get(recordingDir, "jarPaths.dat").toString())))) {
                jarPaths.writeObject(jarsManifest);
            } catch (IOException e) {
                throw new Error("Failed to create jarPaths.dat", e);
            }*/

        /*convert marked jars, e.g., "$REC/path/to/jarname.jar to $REC/jarname.jar
          we do this so the recording directory can be changed between record and replay
          A replay can be moved to a new directory without breaking it*/

            /*jarsManifest.forEach((rawPath, alteredPath) -> {
                if (alteredPath.startsWith("$REC")) { //a jar that we want to copy, not included in jvm
                    Path jarPath = Paths.get(rawPath);
                    Path recJar = Paths.get(recordingDir).resolve(Paths.get(alteredPath).getFileName());
                    try {
                        /* NOTE: an effectful clinit is run as the result of this code
                         * hence why the `flushing` flag and passthrough exists
                         **
                        Files.copy(jarPath, recJar, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new Error("Failed to copy " + jarPath + " to " + recJar, e);
                    }
                }
            }); */

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
            /*if (closeFiles) {
                synchronized (FollowerRecorder.class) {
                    try {
                        classes.close();
                        clinitOut.close();
                    } catch (IOException _) {
                    }
                }
            }*/
        }finally {
            JMVXRuntime.exit();
            flushing = false;
        }
    }

    @Override
    public void systemExit(int status) {
        exitLogic();
        synchronized (connections) {
            for (AFUNIXSocket sock : connections) {
                try {
                    sock.close();
                } catch (IOException e) {
                    log.info("Failed to close a socket in Follower.systemExit");
                    //e.printStackTrace();
                }
            }
        }
    }

    ReadRecorderUtility readRecorder = new ReadRecorderUtility(avoidObjectSerialization, false, false); //true);
    ReadReplayerUtility readReplayer = new ReadReplayerUtility(avoidObjectSerialization, circularBufferCommunicationEnabled);

    @Override
    public int read(JMVXRandomAccessFile f) throws IOException {
        if(isSkippableRandomAccessRead(f)) {
            int ret = f.$JMVX$read();
            readRecorder.recordReadByte(toLogFile, null,
                    markElidedEvents ? OptimizedEvent.ELIDED_RANDOM_ACCESS_READ : OptimizedEvent.RANDOM_ACCESS_READ, ret);
            return ret;
        } else {
            try {
                int ret = readReplayer.replayReadByte(fromCoordinator, buffer, true);
                readRecorder.recordReadByte(toLogFile, null, OptimizedEvent.RANDOM_ACCESS_READ, ret);
                return ret;
            } catch (IOException e) {
                    readRecorder.logException(toLogFile, e);
                    throw e;
            } catch (DivergenceError e) {
                //Divergence handler should go here, I've broken it by pushing the IO into readReplayer
                /*Callable<Object> function = () -> {
                    return JMVXRuntime.read((RandomAccessFile) f);
                };

                DivergenceStatus<Object> divergenceStatus = divergence(Leader.RARead.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

                if (divergenceStatus.containsException) {
                    throw (SecurityException) divergenceStatus.returnValue;
                }
                log.warn("The follower is trying to read from a random access file ()" + f.getClass().getSimpleName());

                return (int) divergenceStatus.returnValue;*/
                throw e;
            }
        }
    }

    private boolean isSkippableRandomAccessRead(JMVXRandomAccessFile f) {
        return readsFromDisk && fastPathRandomAccessFiles.get(f) != null;
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes) throws IOException {
        if (isSkippableRandomAccessRead(f)) {
            int ret = f.$JMVX$read(bytes);
            readRecorder.recordRead(toLogFile, null,
                    markElidedEvents ? OptimizedEvent.ELIDED_RANDOM_ACCESS_READ : OptimizedEvent.RANDOM_ACCESS_READ,
                    bytes, 0, bytes.length, ret);
            return ret;
        } else {
            try {
            int ret = readReplayer.replayRead(fromCoordinator, buffer, true,
                    bytes, 0, bytes.length);
            readRecorder.recordRead(toLogFile, buffer, OptimizedEvent.RANDOM_ACCESS_READ,
                    bytes, 0, bytes.length, ret);
            return ret;
            }catch (IOException e){
                readRecorder.logException(toLogFile, e);
                throw e;
            }catch (DivergenceError e){
                throw e;
                /*Broken
                Callable<Object> function = () -> {
                    return JMVXRuntime.read((RandomAccessFile) f, bytes);
                };

                DivergenceStatus<Object> divergenceStatus = divergence(Leader.RAReadB.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

                if (divergenceStatus.containsException) {
                    throw (SecurityException)divergenceStatus.returnValue;
                }
                log.warn("The follower is trying to read from a random access file (b[]) " + f.getClass().getSimpleName());

                return (int)divergenceStatus.returnValue;
                 */
            }
        }
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes, int off, int len) throws IOException {
        if (isSkippableRandomAccessRead(f)) {
            int ret = f.$JMVX$read(bytes, off, len);
            readRecorder.recordRead(toLogFile, null,
                    markElidedEvents ? OptimizedEvent.ELIDED_RANDOM_ACCESS_READ : OptimizedEvent.RANDOM_ACCESS_READ,
                    bytes, off, len, ret);
            return ret;
        } else {
            try {
            int ret = readReplayer.replayRead(fromCoordinator, buffer, true,
                    bytes, off, len);
            readRecorder.recordRead(toLogFile, buffer, OptimizedEvent.RANDOM_ACCESS_READ,
                    bytes, off, len, ret);
            return ret;
            }catch (IOException e){
                readRecorder.logException(toLogFile, e);
                throw e;
            }catch (DivergenceError e) {
                throw e;
                /*Callable<Object> function = () -> {
                    return JMVXRuntime.read((RandomAccessFile) f, bytes, off, len);
                };

                DivergenceStatus<Object> divergenceStatus = divergence(Leader.RAReadBII.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

                if (divergenceStatus.containsException) {
                    throw (SecurityException)divergenceStatus.returnValue;
                }
                log.warn("The follower is trying to read from random access file (b[], off, len)" + f.getClass().getSimpleName());

                return (int)divergenceStatus.returnValue;*/
            }
        }
    }

    //really using it as a set
    private static ConcurrentHashMap<JMVXRandomAccessFile, Integer> fastPathRandomAccessFiles = new ConcurrentHashMap<>();
    private static final int RD_ONLY_FLAG = 1;

    @Override
    public void open(JMVXRandomAccessFile raf, String name, int mode) throws FileNotFoundException {
        Object fileRAF;
        fileRAF = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) fileRAF);

        if (fileRAF instanceof Leader.RAOpen) {
            Leader.RAOpen fiso = (Leader.RAOpen) fileRAF;
            String n = fiso.name;
            int m = fiso.mode;
            if((m & RD_ONLY_FLAG) != 0){ //safe to read
                fastPathRandomAccessFiles.put(raf, 1);
                raf.$JMVX$open(name, m); //need to open for reading
                return;
            }else if (!n.equals(name) || m != mode) { //divergence
                log.warn("Potential mismatch! Difference in files opened");
                log.warn("The leader tried to open: " + n + " with mode " + Integer.toString(m));
                log.warn("The follower is trying to open: " + name + " with mode " + Integer.toString(mode));
                throw new DivergenceError();
            }else{
                raf.$JMVX$open("/dev/null", m); //redirect
            }
            //raf.$JMVX$open(name, m);
            //TODO, need to extend to track random access files as well as input/output streams
            //JMVXRuntime.openedFiles.put(raf, name);
        } else if (fileRAF instanceof FileNotFoundException) {
            throw ((FileNotFoundException) fileRAF);
        } else {
            divergence(Leader.RAOpen.class.getSimpleName(), fileRAF.getClass().getSimpleName());

            Callable<Object> function = () -> {
                JMVXRuntime.open((RandomAccessFile) raf, name, mode);
                return true;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.RAOpen.class.getSimpleName(), fileRAF.getClass().getSimpleName(), fileRAF, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to open a random access file " + raf.getClass().getSimpleName());
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
        Object write;
        if (circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                OptimizedEvent tag = getTagFromBuffer();
                byte[] data;
                switch (tag){
                    case RANDOM_ACCESS_WRITE:
                        data = buffer.readData(true);
                        int rb = JMVXRuntime.unsafe.getInt(data, Unsafe.ARRAY_BYTE_BASE_OFFSET);
                        verifyWriteOb(raf, b, rb);
                        logWriteOperation(OptimizedEvent.RANDOM_ACCESS_WRITE, rb);
                        return;
                    case WRITE_NO_VALIDATION:
                        if(delayedWriteRecording){
                            //log our copy of the bytes
                            logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_RANDOM_ACCESS_WRITE: OptimizedEvent.RANDOM_ACCESS_WRITE, b);
                            //maybeWrite(raf, b);
                        }else{
                            OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
                        }
                        return;
                    case EXCEPTION:
                        write = receiveObjectFromCoordinator();
                        break; //continues to serialized data code which logs and handles the exception
                    default:
                        throw new DivergenceError();
                }
            }else {
                write = readFromBuffer();
            }
        } else {
            write = receiveObjectFromCoordinator();
        }

        sendObjectToLogFile((Serializable) write);

        if (write instanceof Leader.RAWrite) {
            Leader.RAWrite w = (Leader.RAWrite) write;
            int rb = w.b;
            verifyWriteOb(raf, b, rb);
        } else if (write instanceof IOException) {
            throw ((IOException) write);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.write((RandomAccessFile) raf, b);
                return true;
            };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), write.getClass().getSimpleName(), write, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write the character: " + (char) b);
            return;
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes) throws IOException {
        Object write;
        if(circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                OptimizedEvent tag = getTagFromBuffer();
                switch (tag){
                    case RANDOM_ACCESS_WRITE:
                        byte[] receivedBytes = buffer.readData(true);
                        verifyWriteOb(raf, bytes, receivedBytes, null);
                        logWriteOperation(OptimizedEvent.RANDOM_ACCESS_WRITE, receivedBytes);
                        return;
                    case WRITE_NO_VALIDATION:
                        if(delayedWriteRecording){
                            logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_RANDOM_ACCESS_WRITE: OptimizedEvent.RANDOM_ACCESS_WRITE, bytes);
                            //maybeWrite(os, bytes);
                        }else{
                            OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
                        }
                        return;
                    case EXCEPTION:
                        write = receiveObjectFromCoordinator();
                        break;
                    default:
                        throw new DivergenceError();
                }
            } else {
                write = readFromBuffer();
            }
        } else {
            write = receiveObjectFromCoordinator();
        }

        sendObjectToLogFile((Serializable) write);

        if (write instanceof Leader.RAWriteB) {
            Leader.RAWriteB w = (Leader.RAWriteB) write;
            byte[] b = w.b;
            verifyWriteOb(raf, bytes, b, write);
        } else if (write instanceof IOException) {
            throw ((IOException) write);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.write((RandomAccessFile) raf, bytes);
                return true;
            };
            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = bytes;
            metadata.isOffsetPresent = false;
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.RAWriteB.class.getSimpleName(), write.getClass().getSimpleName(), write, function, metadata);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write the string: " + new String(bytes));
            return;
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes, int off, int len) throws IOException {
        Object write;
        if(circularBufferCommunicationEnabled) {
            if(avoidObjectSerialization) {
                OptimizedEvent tag = getTagFromBuffer();
                switch(tag){
                    case RANDOM_ACCESS_WRITE:
                        int byteOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
                        byte[] data = buffer.readData(true);
                        long byteLen = JMVXRuntime.unsafe.getLong(data, byteOffset);
                        byte[] receivedB =  new byte[(int)byteLen];
                        long otherFieldsStart = byteOffset + Long.BYTES + byteLen;

                        JMVXRuntime.unsafe.copyMemory(data, byteOffset + Long.BYTES, receivedB, byteOffset, byteLen);
                        int receivedOff = JMVXRuntime.unsafe.getInt(data, otherFieldsStart);
                        int receivedLen = JMVXRuntime.unsafe.getInt(data, otherFieldsStart + Integer.BYTES);

                        verifyWriteOb(raf, bytes, receivedB, off, receivedOff, len, receivedLen, null);
                        logWriteOperation(OptimizedEvent.RANDOM_ACCESS_WRITE, bytes, receivedOff, receivedLen);
                        return;
                    case WRITE_NO_VALIDATION:
                        if(delayedWriteRecording){
                            //log our copy of the data
                            logWriteOperation(markElidedEvents ? OptimizedEvent.ELIDED_RANDOM_ACCESS_WRITE: OptimizedEvent.RANDOM_ACCESS_WRITE, bytes, off, len);
                            //maybeWrite(raf, bytes, off, len);
                        }else{
                            OptimizedEvent.writeTag(OptimizedEvent.WRITE_NO_VALIDATION, toLogFile);
                        }
                        return;
                    case EXCEPTION:
                        write = receiveObjectFromCoordinator(); //fallthrough to log to file and handle
                        break;
                    default:
                        throw new DivergenceError();
                }
            } else {
                write = readFromBuffer();
            }
        } else {
            write = receiveObjectFromCoordinator();
        }

        sendObjectToLogFile((Serializable) write);

        if (write instanceof Leader.RAWriteBII) {
            Leader.RAWriteBII w = (Leader.RAWriteBII) write;
            verifyWriteOb(raf, bytes, w.b, off, w.off, len, w.len, write);
        } else if (write instanceof IOException) {
            throw ((IOException) write);
        } else {
            Callable<Object> function = () -> {
                JMVXRuntime.write((RandomAccessFile) raf, bytes, off, len);
                return true;
            };
            Metadata metadata = new Metadata();
            metadata.source = DivergenceSource.FROM_WRITE;
            metadata.buffer = bytes;
            metadata.length = len;
            metadata.offset = off;
            metadata.isOffsetPresent = false;
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.Write.class.getSimpleName(), write.getClass().getSimpleName(), write, function);

            if (divergenceStatus.containsException) {
                throw (IOException) divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to write the string: " + new String(bytes, off, len));
        }
    }

    @Override
    public long length(JMVXRandomAccessFile f) throws IOException {
        long ret = 0;
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.RALength){
            ret = ((Leader.RALength)read).len;
        }else if(read instanceof IOException){
            throw((IOException) read);
        }else{
            Callable<Object> function = () -> {
                return JMVXRuntime.length((RandomAccessFile) f);
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.RALength.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to get the length of a random access file " + read.getClass().getSimpleName());

            return (long) divergenceStatus.returnValue;
        }
        return ret;
    }

    @Override
    public void setLength(JMVXRandomAccessFile f, long newLength) throws IOException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.RASetLength){
            //pass
        }else if(read instanceof IOException){
            throw((IOException) read);
        }else{
            Callable<Object> function = () -> {
                JMVXRuntime.setLength((RandomAccessFile) f, newLength);
                return true;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.RASetLength.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to set the length of a random access file " + read.getClass().getSimpleName());
        }
    }

    @Override
    public void seek(JMVXRandomAccessFile f, long pos) throws IOException {
        Object read;

        if(isSkippableRandomAccessRead(f)){
            f.$JMVX$seek(pos);
            return;
        }

        if (circularBufferCommunicationEnabled) {
            if (avoidObjectSerialization) {
                int byteOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
                byte[] data = buffer.readData(true);
                long byteLen = JMVXRuntime.unsafe.getLong(data, byteOffset);
                int recievedLen = JMVXRuntime.unsafe.getInt(data, byteOffset + Long.BYTES + byteLen  + Integer.BYTES);

                byte[] bytes = new byte[8];
                JMVXRuntime.unsafe.copyMemory(data, byteOffset + Long.BYTES, bytes, byteOffset, 8);

                long l =  ((long) bytes[7] << 56)
                        | ((long) bytes[6] & 0xff) << 48
                        | ((long) bytes[5] & 0xff) << 40
                        | ((long) bytes[4] & 0xff) << 32
                        | ((long) bytes[3] & 0xff) << 24
                        | ((long) bytes[2] & 0xff) << 16
                        | ((long) bytes[1] & 0xff) << 8
                        | ((long) bytes[0] & 0xff);

                // TODO check that l matches pos for divergence
                // NOTE: THE RECORDER DOES NOT WRITE THIS EVENT AT ALL WHEN AVOIDING SERIALIZATION
                // SO WE DON'T HERE EITHER
                return;
            } else {
                read = readFromBuffer();
            }
        } else {
            read = receiveObjectFromCoordinator();
        }


        sendObjectToLogFile((Serializable) read);

        if(read instanceof Leader.RASeek){
            //pass
            //TODO check for divergence on pos argument
        }else if(read instanceof IOException){
            throw((IOException) read);
        }else{
            Callable<Object> function = () -> {
                JMVXRuntime.seek((RandomAccessFile) f, pos);
                return true;
            };

            DivergenceStatus<Object> divergenceStatus = divergence(Leader.RASeek.class.getSimpleName(), read.getClass().getSimpleName(), read, function);

            if (divergenceStatus.containsException) {
                throw (SecurityException)divergenceStatus.returnValue;
            }
            log.warn("The follower is trying to seek in a random access file" + read.getClass().getSimpleName());
        }
    }

    /* TODO? Add readsFromDisk optimization
     * Right now, readsFromDisk works for FileChannel on accident
     * We get a ChannelInputStream.read(byte[]) with goes to InputStream.read(byte[]) into JMVX
     * From JMVX, we determine i should read from disk and end up in this method from the SingleLeader strategy
     * So we skip it, but not in the way we had intended. */
    @Override
    public int read(JMVXFileChannel c, ByteBuffer dst) throws IOException {
        int ret = 0;
        Object read = circularBufferCommunicationEnabled ? readFromBuffer() : receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.ChannelRead){
            Leader.ChannelRead obj = (Leader.ChannelRead) read;
            dst.put(obj.dst);
            ret = obj.ret;
        }else if(read instanceof IOException){
            throw (IOException) read;
        }else{
            Callable<Object> func = () -> c.$JMVX$read(dst);
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.ChannelRead.class.getSimpleName(), read.getClass().getSimpleName(), read, func);
            if(divergenceStatus.containsException){
                throw (IOException) divergenceStatus.returnValue;
            }
            ret = (int)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public long size(JMVXFileChannel c) throws IOException {
        long ret = 0;
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.ChannelSize){
            ret = ((Leader.ChannelSize)read).size;
        }else if(read instanceof IOException){
            throw (IOException) read;
        }else{
            Callable<Object> function = () -> c.$JMVX$size();
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.ChannelSize.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (IOException)divergenceStatus.returnValue;
            }
            ret = (long)divergenceStatus.returnValue;
        }

        return ret;
    }

    @Override
    public long currentTimeMillis() {
        if (avoidObjectSerialization && circularBufferCommunicationEnabled) {
            byte[] b = new byte[8];
            this.buffer.readData(b, 0, b.length);
            long ret = ((long) b[7] << 56)
                    | ((long) b[6] & 0xff) << 48
                    | ((long) b[5] & 0xff) << 40
                    | ((long) b[4] & 0xff) << 32
                    | ((long) b[3] & 0xff) << 24
                    | ((long) b[2] & 0xff) << 16
                    | ((long) b[1] & 0xff) << 8
                    | ((long) b[0] & 0xff);

            try {
                OptimizedEvent.writeTag(OptimizedEvent.CURRENT_TIME, toLogFile);
                toLogFile.writeInt(Long.BYTES);
                //... Dave's poor decision making. Going to writeLong and our code use different endianness
                //so the long has to get re-encoded here (rather than just writing the byte array we have)
                toLogFile.writeLong(ret);
            } catch (IOException e) {
                throw new Error(e);
            }
            return ret;
        } else {
            //Does this work properly when circularBuffer = true and avoidObjSerialization = false?
            //other methods use readFromBuffer for that case...
            Object read = receiveObjectFromCoordinator();
            sendObjectToLogFile((Serializable) read);
            if(read instanceof Leader.TimeMillis){
                return ((Leader.TimeMillis) read).millis;
            }else{
                Callable<Object> function = () -> System.currentTimeMillis();
                DivergenceStatus<Object> divergenceStatus = divergence(Leader.TimeMillis.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
                return (long)divergenceStatus.returnValue;
            }
        }
    }

    @Override
    public long nanoTime() {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NanoTime){
            return ((Leader.NanoTime) read).nano;
        }else{
            Callable<Object> function = () -> System.nanoTime();
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NanoTime.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            return (long)divergenceStatus.returnValue;
        }
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix, FileAttribute<?>... attrs) throws IOException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.CreateTempFile){
            return (((Leader.CreateTempFile) read).ret).toPath();
        }else{
            Callable<Object> function = () -> Files.createTempFile(dir, prefix, suffix, attrs);
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.CreateTempFile.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            return (Path)divergenceStatus.returnValue;
        }
    }

    @Override
    public Path createTempFile(String prefix, String suffix, FileAttribute<?>... attrs) throws IOException {
        return createTempFile(null, prefix, suffix, attrs);
    }

    @Override
    public void copy(JMVXFileSystemProvider fsp, Path source, Path target, CopyOption... options) throws IOException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.FileSystemProviderCopy){
            return;
        }else{
            Callable<Object> function = () -> { fsp.$JMVX$copy(source, target, options); return null; };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileSystemProviderCopy.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
        }
    }

    @Override
    public void checkAccess(JMVXFileSystemProvider fsp, Path path, AccessMode... modes) throws IOException {
        Object check = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) check);
        if(check instanceof Leader.FileSystemProviderCheckAccess){
            IOException e = ((Leader.FileSystemProviderCheckAccess) check).e;
            if (e != null)
                throw e;
        }else{
            Callable<Object> function = () -> { fsp.$JMVX$checkAccess(path, modes); return null; };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileSystemProviderCheckAccess.class.getSimpleName(), check.getClass().getSimpleName(), check, function);
        }
    }

    @Override
    public int open(UnixPath path, int flags, int mode) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeOpen){
            Leader.NativeOpen open = (Leader.NativeOpen) read;
            if(!open.path.equals(path.toString()))
                throw new DivergenceError();
            return UnixNativeDispatcher.open(path, flags, mode);
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> UnixNativeDispatcher.open(path, flags, mode);
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeOpen.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            return (int)divergenceStatus.returnValue;
        }
    }

    @Override
    public void stat(UnixPath path, UnixFileAttributes attrs) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeStat){
            Leader.NativeStat stat = (Leader.NativeStat) read;
            UnixFileAttributesUtil.copy(stat.attrs, attrs);
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { UnixNativeDispatcher.stat(path, attrs); return true; };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeStat.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            //no need to copy to attrs, stat should have done that already
        }
    }

    @Override
    public void lstat(UnixPath path, UnixFileAttributes attrs) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeLstat){
            Leader.NativeLstat lstat = (Leader.NativeLstat) read;
            UnixFileAttributesUtil.copy(lstat.attrs, attrs);
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { UnixNativeDispatcher.stat(path, attrs); return true; };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeLstat.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
        }
    }


    @Override
    public long opendir(UnixPath path) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeOpenDir){
            Leader.NativeOpenDir obj = (Leader.NativeOpenDir) read;
            assert path.toString().equals(obj.path);
            return UnixNativeDispatcher.opendir(UnixPath.toUnixPath(EMPTY_DIR));
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { return UnixNativeDispatcher.opendir(path); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeOpenDir.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            return (long)divergenceStatus.returnValue;
        }
    }

    @Override
    public byte[] readdir(long dir) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeReadDir){
            Leader.NativeReadDir obj = (Leader.NativeReadDir) read;
            assert dir == obj.dir;
            return obj.ret;
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { return UnixNativeDispatcher.readdir(dir); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeReadDir.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            return (byte[])divergenceStatus.returnValue;
        }
    }

    @Override
    public void access(UnixPath path, int amode) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeAccess){
            Leader.NativeAccess obj = (Leader.NativeAccess) read;
            assert (path.toString().equals(obj.path) && amode == obj.amode);
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { UnixNativeDispatcher.access(path, amode); return true; };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeAccess.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
        }
    }

    @Override
    public void closedir(long dir) throws UnixException {
        UnixNativeDispatcher.closedir(dir);
    }

    @Override
    public void mkdir(UnixPath path, int mode) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeMkdir){
            Leader.NativeMkdir obj = (Leader.NativeMkdir) read;
            assert (path.toString().equals(obj.path) && mode == obj.mode);
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { UnixNativeDispatcher.mkdir(path, mode); return true; };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeMkdir.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
        }
    }

    @Override
    public int dup(int fd) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeDup){
            Leader.NativeDup obj = (Leader.NativeDup) read;
            assert fd == obj.fd;
            return UnixNativeDispatcher.dup(fd);
            //return obj.ret; //this could collide with an fd number that has been assigned...?
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { return UnixNativeDispatcher.dup(fd); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeDup.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            return (int)divergenceStatus.returnValue;
        }
    }

    @Override
    public long fdopendir(int dfd) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeFdOpenDir){
            Leader.NativeFdOpenDir obj = (Leader.NativeFdOpenDir) read;
            assert dfd == obj.dfd;
            return UnixNativeDispatcher.opendir(UnixPath.toUnixPath(EMPTY_DIR));//obj.ret;
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { return UnixNativeDispatcher.fdopendir(dfd); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeFdOpenDir.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            return (long)divergenceStatus.returnValue;
        }
    }

    @Override
    public byte[] realpath(UnixPath path) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.NativeRealPath){
            Leader.NativeRealPath obj = (Leader.NativeRealPath) read;
            assert path.equals(obj.path);
            return obj.ret;
        }else if (read instanceof UnixException){
            throw (UnixException) read;
        }else{
            Callable<Object> function = () -> { return UnixNativeDispatcher.realpath(path); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.NativeRealPath.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (UnixException)divergenceStatus.returnValue;
            }
            return (byte[])divergenceStatus.returnValue;
        }
    }

    @Override
    public int read(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented"); //handled by JMVXFileChannel read earlier
    }

    @Override
    public int pread(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len, long position) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long readv(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int write(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int pwrite(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len, long position) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long writev(JMVXFileDispatcherImpl impl, FileDescriptor fd, long address, int len) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long seek(JMVXFileDispatcherImpl impl, FileDescriptor fd, long offset) throws IOException {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.FileDispatcherSeek){
            Leader.FileDispatcherSeek obj = (Leader.FileDispatcherSeek) read;
            assert FileDescriptorUtils.fdAccess.get(fd) == obj.fd && offset == obj.offset;
            return obj.ret;
        }else if (read instanceof IOException){
            throw (IOException) read;
        }else{
            Callable<Object> function = () -> { return impl.$JMVX$seek(fd, offset); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.FileDispatcherSeek.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            if(divergenceStatus.containsException){
                throw (IOException)divergenceStatus.returnValue;
            }
            return (long)divergenceStatus.returnValue;
        }
    }

    @Override
    public int force(JMVXFileDispatcherImpl impl, FileDescriptor fd, boolean metaData) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int truncate(JMVXFileDispatcherImpl impl, FileDescriptor fd, long size) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long size(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int lock(JMVXFileDispatcherImpl impl, FileDescriptor fd, boolean blocking, long pos, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void release(JMVXFileDispatcherImpl impl, FileDescriptor fd, long pos, long size) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void close(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        impl.$JMVX$close(fd);
    }

    @Override
    public void preClose(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getSystemTimeZoneID(String javaHome) {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof Leader.GetSystemTimeZoneID){
            Leader.GetSystemTimeZoneID obj = (Leader.GetSystemTimeZoneID) read;
            assert obj.javaHome.equals(javaHome);
            return obj.ret;
        }else {
            Callable<Object> function = () -> { return TimeZone.getSystemTimeZoneID(javaHome); };
            DivergenceStatus<Object> divergenceStatus = divergence(Leader.GetSystemTimeZoneID.class.getSimpleName(), read.getClass().getSimpleName(), read, function);
            return (String) divergenceStatus.returnValue;
        }
    }

    public static Path getPoolPath(JMVXThreadPoolExecutor pool){
        /* A note about locales, formatting a string with a number (specifically 0) and the default locale
         * can lead to reloading the locale--even though we load the locale once in JMVXRuntime when
         * initializing the strategy.
         * However, the US locale has a passthrough where it returns a character (and thus no loads occur)
         * Therefore, no load can occur.
         */
        return Paths.get(recordingDir,
                String.format(Locale.US, "pool-%d", pool.getPoolId()));
    }

    public static Path getPoolPath(JMVXJob jobInPool){
        return Paths.get(recordingDir,
                String.format(Locale.US, "pool-%d", jobInPool.getPoolId()));
    }

    public static Path getJobPath(JMVXJob job){
        return getPoolPath(job).resolve(String.format(Locale.US, "recording.worker%d.dat", job.$JMVX$getClock()));
    }

    private void createPoolRecordingDir(JMVXThreadPoolExecutor pool){
        /* need to enter/exit because this is called from an event that has no enter/exit
           We don't want ReentrantStrategy to treat mkdirs as the "toplevel" event
         */
        try {
            JMVXRuntime.enter();
            File poolDir = getPoolPath(pool).toFile();
            if (!poolDir.mkdirs())
                log.warn("Failed to make recording directory for pool");
        }finally {
            JMVXRuntime.exit();
        }
    }

    @Override
    public ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
        JMVXThreadPoolExecutor pool = new MVXSafeExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
        createPoolRecordingDir(pool);
        return pool;
    }

    @Override
    public ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        JMVXThreadPoolExecutor pool = new MVXSafeExecutor(0, Integer.MAX_VALUE,
                0L, TimeUnit.MILLISECONDS, //no timeout, kill a worker manually if we run out of jobs
                new JMVXThreadFactory(Optional.ofNullable(threadFactory)));
        createPoolRecordingDir(pool);
        return pool;
    }

    @Override
    public void beforeExecute(Thread t, JMVXJob r) {
        //follower blocks here..., we need to write something when the worker quits
        Object read = receiveObjectFromCoordinator();
        if(read instanceof Leader.JobStarted){
            Leader.JobStarted jobInfo = (Leader.JobStarted) read;
            r.$JMVX$setClock(jobInfo.stamp);
            MVXSafeExecutor exec = (MVXSafeExecutor) r.getPool();
            JMVXJob realJob = exec.takeJobWithStamp(jobInfo.stamp);
            r.setJob(realJob);

            /* record the task we're going to execute */
            //we cannot switch between workers at this moment
            String fname = getJobPath(realJob).toString();
            try {
                JMVXRuntime.enter();
                ObjectOutputStream newStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fname)));
                //Paths.get(recordingDir, fname).toString())));
                newStream.writeInt(vectorClockIndex);
                newStream.flush();
                streamsToFlush.put(fname, newStream);
                logFileObjectStreams.push(toCoordinator); //workerBackup = toCoordinator; //need to preserve this
                toLogFile = newStream;
                //log.info(String.format("TASK %d, IDX %d", r.$JMVX$getClock(), vectorClockIndex));
            } catch (IOException e) {
                throw new Error(e);
            } finally {
                JMVXRuntime.exit();
            }
        }else if(read.equals(CoordinatorThread.exitMessage)) {
            //activates our failsafe, thread would be blocking forever, so we kill it
            throw new JMVXThreadFactory.TerminateException("Leader worker ended, killing follower worker");
        }else{
            throw new DivergenceError();
        }
    }

    @Override
    public void afterExecute(JMVXJob r, Throwable t) {
        //have to have a stream to restore
        if(logFileObjectStreams.isEmpty())
            throw new RuntimeException("Cannot restore log file stream");
        //super.afterExecute(r, t);
        //stream we "lose" was already added to streamsToFlush
        //and is handled later
        //do we want to flush here instead of at the end?
        toLogFile = logFileObjectStreams.pop();
    }

    @Override
    public void submit(JMVXJob task) {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof RecorderWithPools.JobCreated) {
            task.$JMVX$setClock(((RecorderWithPools.JobCreated)read).id);
        }else{
            throw new DivergenceError();
        }
    }

    @Override
    public void runnableFutureDone(JMVXQueueingFuture future) {
        Object read = receiveObjectFromCoordinator();
        sendObjectToLogFile((Serializable) read);
        if(read instanceof RecorderWithPools.TaskFinished){
            future.$JMVX$setClock(((RecorderWithPools.TaskFinished)read).stamp);
        }else{
            throw new DivergenceError();
        }
    }
}

