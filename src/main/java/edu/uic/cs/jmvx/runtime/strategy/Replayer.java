package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.Utils;
import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.*;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;
import edu.uic.cs.jmvx.runtime.util.PreloadClassUtility;
import edu.uic.cs.jmvx.runtime.util.recording.ReadReplayerUtility;
import org.apache.log4j.Logger;
import sun.nio.fs.UnixException;
import sun.nio.fs.UnixNativeDispatcher;
import sun.nio.fs.UnixPath;

import java.io.*;
import java.lang.ref.WeakReference;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarFile;
import java.util.zip.CRC32;

public class Replayer extends Follower{

    // Classloader name -> Class name -> data
    private static HashMap<String, HashMap<String, List<byte[]>>> classesLoaded = new HashMap<>();
    protected ObjectInputStream objects;

    private Stack classNames = new Stack();
    private Stack classStreams = new Stack();

    private static HashMap<String, byte[]> clinits = new HashMap<>();
    private static HashMap<String, String[]> manifests = new HashMap<>();
    private static volatile int threadsAlive = 1; //main thread is 1

    private static final String recordingDir = JMVXRuntime.parameters.recordingDir;
    private static ConcurrentHashMap<String, String> jarRedirects = new ConcurrentHashMap<>();

    protected ReadReplayerUtility readReplayer;

    //shadow the properties we wish to overwrite
    protected static final boolean checksumCommunicationEnabled = false;
    protected static final boolean readsFromDisk = false;
    protected static final boolean circularBufferCommunicationEnabled = false;
    protected static final boolean coreDumpAtEnd = JMVXRuntime.parameters.coreDumpAtEnd;

    static {
        try {
            JMVXRuntime.enter();
            //mimicking the recorder. Done so these classes are loaded at effectively the same point of execution
            Files.walkFileTree(Paths.get(recordingDir), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    throw new IOException();
                    //return FileVisitResult.TERMINATE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

            populateClassloaderMap(classesLoaded, "recording.classes.dat");
            populateMap(clinits, "recording.clinits.dat");
            populateManifestMap(manifests, "recording.manifests.dat");
            readJarRedirects();
            File recDir = new File(recordingDir);

            //this is likely wrong now
            threadsAlive = recDir.list((dir, name) -> !(name.equals("recording.classes.dat") ||
                    name.equals("recording.manifests.dat") ||
                    name.equals("recording.clinits.dat") ||
                    name.endsWith(".jar"))).length;

            PreloadClassUtility.preloadClasses();
        }catch (IOException e){
            throw new RuntimeException("Failed to process recording directory", e);
        }finally {
            JMVXRuntime.exit();
        }
    }

    private static void readJarRedirects(){
        Path jarPaths = Paths.get(recordingDir, "jarPaths.dat");
        try(ObjectInputStream oIn = new ObjectInputStream(new BufferedInputStream(new FileInputStream(jarPaths.toString())))){
            jarRedirects = (ConcurrentHashMap<String, String>) oIn.readObject();
        } catch (FileNotFoundException e) {
            throw new Error("Can't find " + jarPaths, e);
        } catch (ClassNotFoundException | IOException e) {
            throw new Error("Can't read " + jarPaths, e);
        }
    }

    public static void populateClassloaderMap(HashMap<String, HashMap<String , List<byte[]>>> m, String datFilename) {
        try (FileInputStream fis = new FileInputStream(Paths.get(recordingDir, datFilename).toFile())) {
            BufferedInputStream bis = new BufferedInputStream(fis);
            ObjectInputStream ois = new ObjectInputStream(bis);
            while (bis.available() > 0) {
                String classloaderName = (String) ois.readObject();
                String name = (String) ois.readObject();
                byte[] bytes = (byte[]) ois.readObject();
                HashMap<String, List<byte[]>> clBytes = m.get(classloaderName);
                if (clBytes == null) {
                    clBytes = new HashMap<>();
                    m.put(classloaderName, clBytes);
                }

                List<byte[]> lst = clBytes.get(name);
                if (lst == null) {
                    lst = new LinkedList<>();
                    clBytes.put(name, lst);
                }
                lst.add(bytes);
            }
        } catch (EOFException e){
            //fall through... This occurs when clinits.dat is empty or incomplete. Triggered by the creation of ois.
        } catch (IOException | ClassNotFoundException e) {
            throw new Error(e);
        }
    }

    public static void populateManifestMap(HashMap<String, String[]> m, String datFilename){
        try {
            FileInputStream fis = new FileInputStream(Paths.get(recordingDir, datFilename).toFile());
            BufferedInputStream bis = new BufferedInputStream(fis);
            ObjectInputStream ois = new ObjectInputStream(bis);
            while (bis.available() > 0) {
                String name = (String) ois.readObject();
                String[] items = (String[]) ois.readObject();
                m.putIfAbsent(name, items);
            }
        } catch (EOFException e){
            //fall through... This occurs when clinits.dat is empty or incomplete. Triggered by the creation of ois.
        } catch (IOException | ClassNotFoundException e) {
            throw new Error(e);
        }
    }

    public static void populateMap(HashMap<String, byte[]> m, String datFilename){
        try {
            FileInputStream fis = new FileInputStream(Paths.get(recordingDir, datFilename).toFile());
            BufferedInputStream bis = new BufferedInputStream(fis);
            ObjectInputStream ois = new ObjectInputStream(bis);
            while (bis.available() > 0) {
                String name = (String) ois.readObject();
                byte[] bytes = (byte[]) ois.readObject();
                m.putIfAbsent(name, bytes);
            }
        } catch (EOFException e){
            //fall through... This occurs when clinits.dat is empty or incomplete. Triggered by the creation of ois.
        } catch (IOException | ClassNotFoundException e) {
            throw new Error(e);
        }
    }

    public void matchTag(OptimizedEvent tag, OptimizedEvent expectedTag) throws DivergenceError{
        if(!tag.equals(expectedTag))
            throw new DivergenceError();
    }

    public void readAndMatchTag(OptimizedEvent expectedTag) throws IOException, DivergenceError {
        matchTag(OptimizedEvent.readTag(fromCoordinator), expectedTag);
    }

    @Override
    public void main() {
        try {
            this.objects = new ObjectInputStream(new BufferedInputStream(new FileInputStream(Paths.get(recordingDir,"recording." + Thread.currentThread().getName() + ".dat").toFile())));
            this.fromCoordinator = this.objects;
            vectorClockIndex = this.fromCoordinator.readInt();
            JMVXRuntime.clock.registerNewThread(vectorClockIndex);
        } catch (IOException e) {
            throw new Error(e);
        }
        OutputStream ops;
        AtomicBoolean flag = new AtomicBoolean(false);
        try {
            ops = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    if (flag.get()) {
                        throw new IOException("NOT SUPPORTED");
                    }
                }
            };
            this.toCoordinator = new ObjectOutputStream(ops);
        } catch (IOException e) {
            throw new Error(e);
        }
        flag.set(true);
        super.log = Logger.getLogger(Replayer.class);
        readReplayer = new ReadReplayerUtility(avoidObjectSerialization, circularBufferCommunicationEnabled);
    }

    @Override
    public boolean delete(JMVXFile f) {
        boolean ret = true; // = f.$JMVX$delete();
        try {
            Object file = fromCoordinator.readObject();
            if (file instanceof Leader.FileDelete) {
                ret = ((Leader.FileDelete) file).ret;
            } else {
                divergence(Leader.FileDelete.class.getSimpleName(), file.getClass().getSimpleName());
            }
        } catch (ClassNotFoundException | IOException e) {
            throw new Error(e);
        }
        objectCounter++;
        return ret;
    }

    @Override
    public void open(JMVXFileInputStream fis, String name) throws FileNotFoundException {
        Object fileIS;
        try {
            fileIS = fromCoordinator.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new Error(e);
        }

        if (fileIS instanceof Leader.FileInputStreamOpen) {
            Leader.FileInputStreamOpen fiso = (Leader.FileInputStreamOpen) fileIS;
            String n = fiso.name;
            if (!n.equals(name)) {
                /*
                We need to be more diligent about signaling errors.
                 */
                log.warn("Potential mismatch! Difference in files opened");
                log.warn("The leader tried to open: " + n);
                log.warn("The follower is trying to open: " + name);
            }
//            fis.$JMVX$open(fiso.name);
        } else if (fileIS instanceof FileNotFoundException) {
            throw ((FileNotFoundException) fileIS);
        } else {
            divergence(Leader.FileInputStreamOpen.class.getSimpleName(), fileIS.getClass().getSimpleName());
        }
        objectCounter++;
    }

    private int loading = 0;

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name) throws ClassNotFoundException {
        return this.loadClass(loader, name, true);
    }

    private static ConcurrentHashMap<JMVXClassLoader, ConcurrentHashMap<String, Object>> cache = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Object> empty = new ConcurrentHashMap<>();
    //private int dontLogZip = 0;

    private static final JMVXClassLoader extLoader = (JMVXClassLoader) ClassLoader.getSystemClassLoader().getParent();

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name, boolean resolve) throws ClassNotFoundException {
        //matching lock order of Recorder
        synchronized (loader) {
                Object cached = cache.getOrDefault(loader, empty).get(name);
                if (cached != null) {
                    if (cached instanceof Class<?>)
                        return (Class<?>)cached;
                    else
                        throw (ClassNotFoundException) cached;
                }

                loading++;
                classNames.push(name);

                try{
                    if(name.startsWith("edu.uic.cs.jmvx") && ((ClassLoader)loader).getParent() != null){
                        //skip JMVX Classes
                        try{
                            JMVXRuntime.enter();
                            return extLoader.$JMVX$loadClass(name, resolve);
                        }finally{
                            JMVXRuntime.exit();
                        }
                    }

                    ByteArrayInputStream bis;
                    synchronized (Replayer.class) {
                        HashMap<String, List<byte[]>> loadedByLoader = classesLoaded.get(loader.getClass().getName());
                        if (loadedByLoader == null) { //this loader did not load the class during recording
                            JMVXRuntime.enter();
                            try {
                                return loader.$JMVX$loadClass(name, resolve); //throw exception instead?
                            } finally {
                                JMVXRuntime.exit();
                            }
                        }
                        List<byte[]> lst = loadedByLoader.get(name);
                        if (lst == null) { //handles loading JMVX classes usually
                            JMVXRuntime.enter();
                            try {
                                // Class was not loaded during the recording, attempt to load it now
                                return loader.$JMVX$loadClass(name, resolve);
                            } finally {
                                JMVXRuntime.exit();
                            }
                        }

                        if (lst.size() == 0) //somehow, we've run out of bytes..?!
                            throw new ClassNotFoundException();

                        byte[] bytes = lst.remove(0);

                        JMVXRuntime.enter();
                        bis = new ByteArrayInputStream(bytes);
                    }

                    try {
                        classStreams.push(fromCoordinator);
                        fromCoordinator = new ObjectInputStream(bis);
                        fromCoordinator.readObject();  // Discards the null we wrote first thing to each byte[]
                    } catch (IOException e) {
                        throw new Error(e);
                    } finally {
                        JMVXRuntime.exit();
                    }

                    ConcurrentHashMap<String, Object> m = getClassLoaderCache(loader);

                    try {
                        /*
                        Issue as I see it, if the class order differs, we will still
                        have divergences in reading some raw bytes.
                        We have to read the index of the file somewhere
                        If we try to load java/lang/Thread too early, and it gets
                        kicked to the Dacapo loader, we find ourselves diverging on
                        reading the raw bytes. Even though we have the files to read through,
                        the passthrough doesn't exist. But do we want the passthrough everywhere?
                        Would it be better to simulate divegent ops, eg the searching and replay
                        the ones which should happen everytime (the final read of the class bytes)

                        Maybe we don't need to copy all of the jars, but rather indexes of the jars
                        themselves (eg the output of jar -tf file.jar).
                         */
                        Class<?> ret = loader.$JMVX$loadClass(name, resolve);
                        m.put(name, ret);
                        return ret;
                    } catch (ClassNotFoundException e) {
                        m.put(name, e);
                        throw e;
                    } finally {
                        JMVXRuntime.enter();
                        try {
                            //classNames.pop();
                            //this.fromCoordinator = this.objects;
                            fromCoordinator = (ObjectInputStream) classStreams.pop();
                        } finally {
                            JMVXRuntime.exit();
                        }
                    }

                } finally {
                    classNames.pop();
                    loading--;
                }
            }
    }

    private ConcurrentHashMap<String, Object> getClassLoaderCache(JMVXClassLoader loader) {
        ConcurrentHashMap<String, Object> m = cache.get(loader);
        if (m == null) {
            m = new ConcurrentHashMap<>();
            cache.put(loader, m);
        }
        return m;
    }

    @Override
    public void clinitStart(String cls) {
        inClinit++;
        ByteArrayInputStream bis;
        byte[] bytes;
        JMVXRuntime.enter();
        try {
            bytes = getClinit(cls);

            //push just in case bytes = null
            //if this happens, we still hit clinitExit, which will pop from this stack
            clinitsStreams.push(fromCoordinator);
            //if(bytes == null){ throw new Error("No clinit..."); } //no bytes, can't alter fromCoordinator
            bis = new ByteArrayInputStream(bytes);
            try {
                fromCoordinator = new ObjectInputStream(bis);
                fromCoordinator.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new Error(e);
            }
        } finally {
            JMVXRuntime.exit();
        }
    }

    @Override
    public void clinitEnd(String cls) {
        fromCoordinator = clinitsStreams.pop();
        inClinit--;
    }

    @Override
    protected byte[] getClinit(String cls) {
        return clinits.get(cls);
    }

    @Override
    public void exitLogic() {
        //This code is in exit logic and not threadExit
        //because threadExit misses main's exit.
        //exitLogic is called for main and threads.
        markDead();
    }

    @Override
    public void systemExit(int status) {
        //exitMain to create same sequence of calls the Recorder does
        JMVXRuntime.exitMain(null); //halt(0);
    }

    /* STOP GAP SOLUTION
     * FollowerRecorder optimizes use of RandomAccess files, but the regular Follower does not
     * Override them here until the optimizations are back ported
     */

    @Override
    public int read(JMVXRandomAccessFile f) throws IOException {
        return readReplayer.replayReadByte(fromCoordinator, buffer, true);
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes) throws IOException {
        int ret = readReplayer.replayRead(fromCoordinator, buffer, true,
                bytes, 0, bytes.length);
        return ret;
    }

    @Override
    public int read(JMVXRandomAccessFile f, byte[] bytes, int off, int len) throws IOException {
        int ret = readReplayer.replayRead(fromCoordinator, buffer, true,
                bytes, off, len);
        return ret;
    }

    @Override
    public void open(JMVXRandomAccessFile raf, String name, int mode) throws FileNotFoundException {
        Object fileRAF;
        fileRAF = receiveObjectFromCoordinator();

        if (fileRAF instanceof Leader.RAOpen) {
            Leader.RAOpen fiso = (Leader.RAOpen) fileRAF;
            String n = fiso.name;
            int m = fiso.mode;
            if (!n.equals(name) || m != mode) { //divergence
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
    public void write(JMVXRandomAccessFile raf, int b) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try {
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch(tag){
                case RANDOM_ACCESS_WRITE:
                case ELIDED_RANDOM_ACCESS_WRITE:
                    try {
                        int size = this.fromCoordinator.readInt(); //in bytes
                        if (size != Integer.BYTES)
                            divergence("Difference in number of bytes written", Leader.WriteB.class.getName());
                        int written = this.fromCoordinator.readInt();
                        verifyWriteOb(raf, b, written);
                        return;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case WRITE_NO_VALIDATION:
                    //skip write
                    //maybeWrite(os, b);
                    return;
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            super.write(raf, b);
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try {
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch (tag){
                case RANDOM_ACCESS_WRITE:
                case ELIDED_RANDOM_ACCESS_WRITE:
                    try {
                        int rLen = this.fromCoordinator.readInt();
                        byte[] rBytes = new byte[rLen];
                        this.fromCoordinator.readFully(rBytes);

                        //checks the contents of bytes, not the arguments to the call
                        verifyWriteOb(raf, bytes, rBytes, null);
                        return;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case WRITE_NO_VALIDATION:
                    //skip write
                    //maybeWrite(os, bytes);
                    return;
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            super.write(raf, bytes);
        }
    }

    @Override
    public void write(JMVXRandomAccessFile raf, byte[] bytes, int off, int len) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try {
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch (tag){
                case RANDOM_ACCESS_WRITE:
                case ELIDED_RANDOM_ACCESS_WRITE:
                    /* If everything goes well from here on, we should not get
                    and IOException. If we do, its a problem with JMVX */
                    try {
                        int size = this.fromCoordinator.readInt();
                        int rOff = this.fromCoordinator.readInt();
                        int rLen = this.fromCoordinator.readInt();
                        byte[] rBytes = new byte[rLen];
                        this.fromCoordinator.readFully(rBytes);

                        //this code should be added to verifyWriteOb
                        //if(rOff != off || rLen != len)
                        //    divergence("Arguments to write differ", Leader.WriteBII.class.getName());

                        //checks the contents of bytes, not the arguments to the call
                        verifyWriteOb(raf, bytes, rBytes, off, 0, len, rLen, null);
                        return;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case WRITE_NO_VALIDATION:
                    //skip
                    //maybeWrite(os, bytes, off, len);
                    return;
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            super.write(raf, bytes, off, len);
        }
    }

    protected void verifyWriteOb(JMVXRandomAccessFile raf, int expectedB, int receivedB) throws IOException {
        if (expectedB != receivedB) {
            log.warn("Potential mismatch! Difference in byte written");
            throw new DivergenceError();
        }
        //not doing the write for RandomAccess files
        //maybeWrite(raf, receivedB)
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

    private CRC32 crc32 = new CRC32();

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

    @Override
    public void seek(JMVXRandomAccessFile f, long pos) throws IOException {
        if(avoidObjectSerialization) {
            //f.$JMVX$seek(pos);
            return;
        }
        super.seek(f, pos);
    }

    protected long[] clockCopy = new long[0];
    protected byte[] bytes = new byte[0];

    /*
    Method is difficult to refactor
    The encoding/decoding of the clock differs from the Leader, so we cannot rely on the super for the optimized case
    In addition, this method makes use of the tag to determine HOW to decode the rest of the event.
    Refactoring into a parser/handler combo leads to additional conditional checks
    This code should be FAST, gets called a lot, so rewriting the method (decoder and handler) allows subtle but useful
    changes
     */
    @Override
    public void monitorenter(Object o) {
        /* Uncertain which thread initializes a class, for skip it */
        if (inClinit > 0 || loading > 0) {
            JMVXRuntime.unsafe.monitorEnter(o);
            return;
        }

        if (avoidObjectSerialization) {
            /*
            This try/catch and the one in wait are similar and likely can be merged
            Currently, they are not as monitorenter is a highly used operation
            Leaving it separate reduces the introduction of more conditionals.
             */
            try {
                OptimizedEvent tag = OptimizedEvent.readTag(this.fromCoordinator);
                switch (tag){
                    case SINGLE_THREADED: //avoid vc
                        JMVXRuntime.unsafe.monitorEnter(o);
                        return;
                    case MONITOR_ENTER: //sync needed
                        int size = this.fromCoordinator.readInt();
                        //int hashcode = this.fromCoordinator.readInt();
                        int length = size / Long.BYTES; //(size - Integer.BYTES) / Long.BYTES;

                        if (length > clockCopy.length) {
                            clockCopy = new long[length];
                            bytes = new byte[length * Long.BYTES];
                        }

                        /*
                        The endianness of the system matters.
                        writeLong/readLong writes with a particular endianness
                        So we can do that to strengthen the argument of sending recordings to devs
                         */
                        this.fromCoordinator.readFully(bytes);
                        JMVXRuntime.unsafe.copyMemory(bytes, CircularBuffer.byteOffset, clockCopy, CircularBuffer.longOffset, bytes.length);
                        JMVXRuntime.clock.sync(vectorClockIndex, clockCopy, true);
                        //fallthrough to acquire monitor and increment
                    case FAST_MONITOR_ENTER: //just increment
                        JMVXRuntime.unsafe.monitorEnter(o);
                        JMVXRuntime.clock.increment(vectorClockIndex, null);
                        return;
                    default:
                        throw new DivergenceError();
                }
            }catch (IOException | ClassCastException e){
                throw new Error(e);
            }
        } else {
            super.monitorenter(o);
        }
    }

    @Override
    public void wait(Object o, long timeout, int nanos) throws InterruptedException {
        if (inClinit > 0 || loading > 0) {
            o.wait(timeout, nanos);
            return;
        }

        if (avoidObjectSerialization) {
            try {
                int i = 0;
                // Check tag before we do anything with the object!
                //OptimizedEvent tag = OptimizedEvent.readTag(this.fromCoordinator);
                byte tagByte = this.fromCoordinator.readByte();
                if(tagByte < OptimizedEvent.WAIT.ordinal() ||
                        tagByte > OptimizedEvent.SINGLE_THREADED.ordinal()){
                    throw new DivergenceError();
                }

                //release the object. Do we need to do this for the fast tag?
                //we would just re-acquire in the end anyways
                //is a fast wait even possible? That would mean either we notified ourselves,
                //or we had a spurious wakeup.
                while(Thread.holdsLock(o)) {
                    JMVXRuntime.unsafe.monitorExit(o);
                    i++;
                }

                OptimizedEvent tag = OptimizedEvent.fromByte(tagByte);
                switch (tag){
                    case SINGLE_THREADED: //avoid vc
                        for(; i != 0; i--)
                            JMVXRuntime.unsafe.monitorEnter(o);
                        return;
                    case WAIT: //sync needed
                        int size = this.fromCoordinator.readInt();
                        //int hashcode = this.fromCoordinator.readInt();
                        int length = size / Long.BYTES; //(size - Integer.BYTES) / Long.BYTES;

                        if (length > clockCopy.length) {
                            clockCopy = new long[length];
                            bytes = new byte[length * Long.BYTES];
                        }

                        /*
                        The endianness of the system matters.
                        writeLong/readLong writes with a particular endianness
                        So we can do that to strengthen the argument of sending recordings to devs
                         */
                        this.fromCoordinator.readFully(bytes);
                        JMVXRuntime.unsafe.copyMemory(bytes, CircularBuffer.byteOffset, clockCopy, CircularBuffer.longOffset, bytes.length);
                        JMVXRuntime.clock.sync(vectorClockIndex, clockCopy, true);
                        //fallthrough to acquire monitor and increment
                    case FAST_WAIT: //just increment
                        for(; i != 0; i--)
                            JMVXRuntime.unsafe.monitorEnter(o);
                        JMVXRuntime.clock.increment(vectorClockIndex, null);
                        return;
                    default:
                        throw new DivergenceError();
                }
            } catch (IOException e) {
                throw new Error(e);
            }
        } else{
            super.wait(o, timeout, nanos);
        }
    }

    private int readFastInt() {
        try {
            return this.readFastInt((byte[]) this.fromCoordinator.readObject());
        } catch (ClassNotFoundException | IOException e) {
            throw new Error(e);
        }
    }

    private int readFastInt(byte[] bb) {
        return  ((bb[0] & 0xFF) << 24) |
                ((bb[1] & 0xFF) << 16) |
                ((bb[2] & 0xFF) << 8 ) |
                ((bb[3] & 0xFF) << 0 );
    }

    @Override
    public void write(JMVXOutputStream os, int b) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try {
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch(tag){
                case WRITE:
                case ELIDED_WRITE:
                    try {
                        int size = this.fromCoordinator.readInt(); //in bytes
                        if (size != Integer.BYTES)
                            divergence("Difference in number of bytes written", Leader.WriteB.class.getName());
                        int written = this.fromCoordinator.readInt();
                        verifyWriteOb(os, b, written);
                        return;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case WRITE_NO_VALIDATION:
                    maybeWrite(os, b);
                    return;
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            super.write(os, b);
        }
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try {
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch (tag){
                case WRITE:
                case ELIDED_WRITE:
                    try {
                        int rLen = this.fromCoordinator.readInt();
                        byte[] rBytes = new byte[rLen];
                        this.fromCoordinator.readFully(rBytes);

                        //checks the contents of bytes, not the arguments to the call
                        verifyWriteOb(os, bytes, rBytes, null);
                        return;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case WRITE_NO_VALIDATION:
                    maybeWrite(os, bytes);
                    return;
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            super.write(os, bytes);
        }
    }

    @Override
    public void write(JMVXOutputStream os, byte[] bytes, int off, int len) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try {
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch (tag){
                case WRITE:
                case ELIDED_WRITE:
                    /* If everything goes well from here on, we should not get
                    and IOException. If we do, its a problem with JMVX */
                    try {
                        int size = this.fromCoordinator.readInt();
                        int rOff = this.fromCoordinator.readInt();
                        int rLen = this.fromCoordinator.readInt();
                        byte[] rBytes = new byte[rLen];
                        this.fromCoordinator.readFully(rBytes);

                        //this code should be added to verifyWriteOb
                        //if(rOff != off || rLen != len)
                        //    divergence("Arguments to write differ", Leader.WriteBII.class.getName());

                        //checks the contents of bytes, not the arguments to the call
                        verifyWriteOb(os, bytes, rBytes, off, 0, len, rLen, null);
                        return;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case WRITE_NO_VALIDATION:
                    maybeWrite(os, bytes, off, len);
                    return;
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            super.write(os, bytes, off, len);
        }
    }

    public void connect(JMVXSocket sock, SocketAddress endpoint, int timeout) throws IOException {
        Object obj = receiveObjectFromCoordinator();
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
                    junkServer.accept();
                }
            } catch (Exception e) {
                log.error("Error connecting to junk server");
                e.printStackTrace();
            }
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
        Object obj = receiveObjectFromCoordinator();
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
            try {
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
        Object obj = receiveObjectFromCoordinator();
        /*
        2 possibilities:
        1: We've diverged and are executing code that should run
        2: The call to accept never terminated! Stay blocked forever

        Case 2 actually happens in H2 client server!
        So we are going with that for now. A better strategy may be to write events
        for AcceptBegin and AcceptEnd. AcceptBegin would read the next item,
        if it throws an error -> block forever.
        If it is something else, diverge (not possible b/c accept must return)
        if it is AcceptEnd, we create a new connection and return it.

        We never have to worry about this in leader follower because the leader's call to accept
        never returns, so the follower blocks, albeit while waiting for an object from the coordinator.
         */
        if(obj instanceof Leader.AcceptBegin){
            obj = receiveObjectFromCoordinator();
            if(obj == null) {
                //hack for now. Recorder may record shutdown hooks
                //so we are going to kill threads that would suspend
                markDead();
                Thread.currentThread().stop();
            }else if(obj instanceof Leader.AcceptEnd){
                //accepted a new connection
                synchronized (junkServer){
                    new Socket(junkAddress.getHostName(), junkAddress.getPort());
                    return junkServer.accept();
                }
            }if(obj instanceof IOException){ //throw an exception if we are given one
                throw (IOException) obj;
            }//else fallthrough to diverge
        }
        //else diverge
        Callable<Object> function = () -> {
            return JMVXRuntime.accept((ServerSocket) serv);
        };

        DivergenceStatus<Object> divergenceStatus = divergence(Leader.Accept.class.getSimpleName(), obj.getClass().getSimpleName(), obj, function);

        if (divergenceStatus.containsException) {
            throw (SecurityException) divergenceStatus.returnValue;
        }

        return (Socket) divergenceStatus.returnValue;
    }

    @Override
    public long currentTimeMillis() {
        if (avoidObjectSerialization) {
            try {
                readAndMatchTag(OptimizedEvent.CURRENT_TIME);
                int size = this.fromCoordinator.readInt();
                long ret = this.fromCoordinator.readLong();
                return ret;
            } catch (IOException e) {
                throw new Error(e);
            }
        } else {
            return super.currentTimeMillis();
        }
    }

    private void halt(int status){
        JMVXRuntime.enter();
        //we don't want to run shutdown hooks
        //so we trigger this here instead
        if(coreDumpAtEnd){
            JMVXRuntime.coreDump();
        }
        /*
        Halt exits the JVM without running shutdown hooks
        Shutdown hooks end up being run with SingleLeaderWithoutCoordinator when recording,
        so we don't have proper logs to replay them. It's dangerous to replay them with
        SingleLeaderWithoutCoordinator in case they operate on files that don't exist.
        So we use halt to bypass it. If we have made it here, we've already replayed what we
        were interested in.
        Note: Jython may have strange behavior with the mechanism because it uses a shutdown hook
        to reset terminal properties (e.g., it runs "stty echo" to turn character echoing BACK ON)
        In my experiments, my terminal was fine (I was still able to see typed characters)
         */
        Runtime.getRuntime().halt(status);
    }

    private void markDead(){
        threadsAlive--;
        if(threadsAlive <= 0){
            halt(0);
        }
    }

    @Override
    public int available(JMVXInputStream is) throws IOException {
        if (avoidObjectSerialization) {
            OptimizedEvent tag;
            try{
                tag = OptimizedEvent.readTag(fromCoordinator);
            }catch (IOException e){
                throw new Error(e);
            }

            switch (tag){
                case AVAILABLE:
                case ELIDED_AVAILABLE:
                    try {
                        int size = fromCoordinator.readInt();
                        if(size != Integer.BYTES){
                            throw new DivergenceError();
                        }
                        int ret = fromCoordinator.readInt(); //readFastInt();
                        return ret;
                    }catch (IOException e){
                        throw new Error(e);
                    }
                case EXCEPTION:
                    IOException e = (IOException) receiveObjectFromCoordinator();
                    throw e;
                default:
                    throw new DivergenceError();
            }
        } else {
            return super.available(is);
        }
    }

    protected Object receiveObjectFromCoordinator() {
        Object o = super.receiveObjectFromCoordinator();
        if(o == null){
            markDead();
        }
        return o;
    }

    @Override
    public void initIDs() {
        //nop, we intercept with other methods
        JMVXRuntime.zipfile.$JMVX$initIDs();
    }

    @Override
    public long getEntry(long jzfile, byte[] name, boolean addSlash) {
        if(loading == 0) {
            Object read = receiveObjectFromCoordinator();
            if (read instanceof Recorder.GetEntry) {
                Recorder.GetEntry entry = (Recorder.GetEntry) read;
                if (!Arrays.equals(entry.name, name)) {
                    throw new DivergenceError();
                }

                if (entry.ret > 0) {
                    throw new Error("getEntry should appear in loading");
                }
                return entry.ret;
            } else {
                throw new DivergenceError();
            }
        }else{
            if(jzfile <= 0){
                throw new Error(String.format("Cannot open jzfile %d\n", jzfile));
            }

            long jzentry = JMVXRuntime.zipfile.$JMVX$getEntry(jzfile, name, addSlash);
            return jzentry;
        }
    }

    @Override
    public void freeEntry(long jzfile, long jzentry) {
        /*
         * This could actually get called as part of a read!
         * So read is called, intercepted by JMVX and then freeEntry is called,
         * but goes through because the depth > 1. To counteract this, the reentrent Strategy
         * has changed to always call this method when replaying. In otherwords, we also assume
         * we have a freeEntry object to read and always treat it as a nop.
         */
        if(loading > 0 && jzfile > 0 && jzentry > 0){
            JMVXRuntime.zipfile.$JMVX$freeEntry(jzfile, jzentry);
        }
    }

    @Override
    public long getNextEntry(long jzfile, int i) {
        if(loading > 0){
            long jzentry = JMVXRuntime.zipfile.$JMVX$getNextEntry(jzfile, i);
            return jzentry;
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetNextEntry) {
            //TODO error check
            long jzentry = ((Recorder.GetNextEntry) read).ret;
            return jzentry;
        }else{
            throw new DivergenceError();
        }
    }

    @Override
    public void close(long jzfile) {
        if(loading > 0 && jzfile > 0){
            JMVXRuntime.zipfile.$JMVX$close(jzfile);
        }
    }

    @Override
    public long open(String name, int mode, long lastModified, boolean usemmap) throws IOException {
        long ret = -1L;
        //open something we know we have a redirect for
        String p = jarRedirects.get(name);
        if(p != null){
            if(p.startsWith("$REC")) { //redirect to recordingDir
                String toOpen = Paths.get(recordingDir, Paths.get(p).getFileName().toString()).toString();
                ret = JMVXRuntime.zipfile.$JMVX$open(toOpen, mode, lastModified, usemmap);
            } else if(p.equals(name)) //no redirect necessary, e.g., jar belongs to jdk
                ret = JMVXRuntime.zipfile.$JMVX$open(name, mode, lastModified, usemmap);
        }

        if(loading == 0){ //not loading, we need to consume an event
            Object read = receiveObjectFromCoordinator();
            if(read instanceof Recorder.Open){
                Recorder.Open op = (Recorder.Open) read;
                /*
                set ret pointer if we haven't yet
                This handles the case where we haven't opened anything
                e.g., op opens a zipfile and we want to return a "fake" pointer (op.ret)
                 */
                ret = ret > 0? ret: op.ret;
            }else if(read instanceof IOException){
                throw (IOException) read;
            }else{
                throw new DivergenceError();
            }
        }else{
            if(ret < 0) // should have opened already
                throw new DivergenceError();
        }

        return ret;
    }

    @Override
    public int getTotal(long jzfile) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getTotal(jzfile);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetTotal) {
            //TODO error check
            return ((Recorder.GetTotal) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public boolean startsWithLOC(long jzfile) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$startsWithLOC(jzfile);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.StartsWithLOC) {
            //TODO error check
            return ((Recorder.StartsWithLOC) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public int read(long jzfile, long jzentry, long pos, byte[] b, int off, int len) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$read(jzfile, jzentry, pos, b, off, len);
        }
        //handled by streams...
        //should be wrapped with a call to a ZipFileInputStream
        /*Object read = receiveObjectFromCoordinator();
        if(read instanceof byte[]){
            byte[] bytes = (byte[])read;
            int n = Math.min(bytes.length, len);
            System.arraycopy(bytes, 0, b, off, n);
            return n;
        }else{
            throw new DivergenceError();
        }*/
        throw new DivergenceError();
    }

    @Override
    public long getEntryTime(long jzentry) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntryTime(jzentry);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntryTime) {
            //TODO error check
            return ((Recorder.GetEntryTime) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public long getEntryCrc(long jzentry) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntryCrc(jzentry);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntryCrc) {
            //TODO error check
            return ((Recorder.GetEntryCrc) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public long getEntryCSize(long jzentry) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntryCSize(jzentry);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntryCSize) {
            //TODO error check
            return ((Recorder.GetEntryCSize) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public long getEntrySize(long jzentry) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntrySize(jzentry);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntrySize) {
            //TODO error check
            return ((Recorder.GetEntrySize) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public int getEntryMethod(long jzentry) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntryMethod(jzentry);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntryMethod) {
            //TODO error check
            return ((Recorder.GetEntryMethod) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public int getEntryFlag(long jzentry) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntryFlag(jzentry);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntryFlag) {
            //TODO error check
            return ((Recorder.GetEntryFlag) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public byte[] getCommentBytes(long jzfile) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getCommentBytes(jzfile);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetCommentBytes) {
            //TODO error check
            return ((Recorder.GetCommentBytes) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public byte[] getEntryBytes(long jzentry, int type) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getEntryBytes(jzentry, type);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetEntryBytes) {
            //TODO error check
            return ((Recorder.GetEntryBytes) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public String getZipMessage(long jzfile) {
        if(loading > 0){
            return JMVXRuntime.zipfile.$JMVX$getZipMessage(jzfile);
        }
        Object read = receiveObjectFromCoordinator();
        if (read instanceof Recorder.GetZipMessage) {
            //TODO error check
            return ((Recorder.GetZipMessage) read).ret;
        } else {
            throw new DivergenceError();
        }
    }

    @Override
    public String[] getMetaInfEntryNames(JMVXJarFile jarfile) {
        JarFile jf = (JarFile)jarfile;
        if(loading > 0){
            return jarfile.$JMVX$getMetaInfEntryNames();
        }
        String[] miss = new String[0]; //ptr
        String[] ret = manifests.getOrDefault(jf.getName(), miss);
        //null can be returned from this method!
        //so we compare against some object pointer, e.g., miss
        //to detect a divergence
        if(miss == ret){
            throw new DivergenceError();
        }
        return ret;
    }

    private ConcurrentHashMap<JMVXFile, WeakReference<JMVXFile>> redirectedFiles = new ConcurrentHashMap<>();

    /*
    Redirects files which are to be used for loading
     */
    public JMVXFile redirect(JMVXFile f){
        WeakReference<JMVXFile> ref = redirectedFiles.get(f);
        JMVXFile dest;
        if(ref != null){
            dest = ref.get();
            if(dest != null) //cache hit
                return dest;
            //else miss, and we'll make a new reference
        }

        dest = f; //no redirect is default.

        String name = f.$JMVX$getAbsolutePath();
        String p = jarRedirects.get(name);
        if(name.startsWith(recordingDir)) { //already transformed
            dest = f;
        }else if(p != null){
            if(p.startsWith("$REC")) { //redirect to recordingDir
                Path toOpen = Paths.get(recordingDir, Paths.get(p).getFileName().toString());
                dest = (JMVXFile) toOpen.toFile();
            } /*else if(p.equals(name)) { //no redirect necessary, e.g., jar belongs to jdk
                dest = f;
            }*/
        }
        redirectedFiles.put(f, new WeakReference<>(dest));
        //opened early, before jmvx was init'ed
        return dest;
    }

    @Override
    public boolean exists(JMVXFile f) {
        if(loading == 0) {
            return super.exists(f);
        }else{
            return redirect(f).$JMVX$exists();
        }
    }

    @Override
    public long lastModified(JMVXFile f) {
        if(loading == 0) {
            return super.lastModified(f);
        } else {
            return redirect(f).$JMVX$lastModified();
        }
    }

    @Override
    public int read(JMVXInputStream is) throws IOException {
        return readReplayer.replayReadByte(fromCoordinator, buffer, false);
    }

    @Override
    public int read(JMVXInputStream is, byte[] bytes) throws IOException {
        int ret = readReplayer.replayRead(fromCoordinator, buffer, false,
                bytes, 0, bytes.length);
        return ret;
    }

    @Override
    public int read(JMVXInputStream is, byte[] bytes, int off, int len) throws IOException {
        if (loading > 0) {
            if (is.getClass().equals(JMVXRuntime.zipFileInputStreamClass)) {
                return is.$JMVX$read(bytes, off, len);
            }/* else {
            Loading from a .class
            Currently these are not moved into the recording directory
            The bytes read ARE LOGGED, so fail through
            In the future, when adding more holistic support,
            we would use the passthrough here
             */
        }
        int ret = readReplayer.replayRead(fromCoordinator, buffer, false,
                bytes, off, len);
        return ret;
    }

    /* Exactly the same logic, but we DO NOT open the file as it may not be there */
    @Override
    public int open(UnixPath path, int flags, int mode) throws UnixException {
        Object read = receiveObjectFromCoordinator();
        if(read instanceof Leader.NativeOpen){
            Leader.NativeOpen open = (Leader.NativeOpen) read;
            if(!open.path.equals(path.toString()))
                throw new DivergenceError();
            //provide a valid fd that points to /dev/null instead
            //this allows other methods e.g., dup to still execute
            //albeit they do not rely on the persistence of the original file
            return UnixNativeDispatcher.open(UnixPath.toUnixPath(Paths.get("/dev/null")), flags, mode); //open.ret; 
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

    //not necessary because we open /dev/null instead. If we decide to scrap that, we need to re-enable this
    //and override other methods from UnixNativeDispatcher that rely on fds (e.g., dup)
    /*@Override
    public void close(JMVXFileDispatcherImpl impl, FileDescriptor fd) throws IOException {
        //nop. We add a placeholder fd with open. Closing an invalid descriptor triggers an error
    }*/
}
