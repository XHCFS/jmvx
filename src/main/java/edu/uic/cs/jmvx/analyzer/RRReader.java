package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RRReader implements Consumer<RRVisitor> {
    public static final String LOADER_DAT = "recording.classes.dat";
    public static final String CLINITS_DAT = "recording.clinits.dat";
    private final Path toRecording;
    /*Set of paths to .dat files which are not task files
    These will be processed by visitOther instead of a TaskVisitor
     */
    private static final HashSet<Path> NOT_TASK_FILES = new HashSet<>();
    /*
    Currently, RRReaders create and manage their own thread pools.
    Do we want this? Or should RRReader create a global pool that all readers can use
    And each reader is responsible for waiting on its own jobs.

    Side note: I've answered my question of what creature would use multiple thread pools in a single application
    Apparently: me
     */
    private ExecutorService threadPool;

    static {
        /*converts string versions of the filename to paths
        and adds to the collection of files to skip*/
        NOT_TASK_FILES.addAll(Arrays.stream(new String[]{
                "recording.manifests.dat",
                "jarPaths.dat",
        }).map(Paths::get).collect(Collectors.toList()));
    }

    public RRReader(Path toRecording) {
        this.toRecording = toRecording;
        this.threadPool = Executors.newFixedThreadPool(8);
    }

    private static void readHeader(ObjectInputStream stream, TaskVisitor visitor) throws IOException {
        /*
        Event streams from class loading do not log a position in the vector clock
        So our "header" will not exist. Changes how we have to start reading the stream
        (eg consuming a null vs an int)
        Arguably, we could just insert a sentinel for class loading streams (eg -1 or -2)
         */
        Optional<Integer> header;
        try {
            Object o = stream.readObject();
            if(o != null)
                throw new IOException("Read an object other than a header");
            header = Optional.empty(); //no header
        } catch (ClassNotFoundException e) {
            //error case, something strange is happening here
            throw new IOException("Weird start to a stream", e);
        } catch (OptionalDataException _){
            try {
                int clockIndex = stream.readInt();
                header = Optional.of(clockIndex); //clock index is header
            }catch (IOException e){
                throw new IOException("Stream doesn't start with an Integer header", e);
            }
        }

        visitor.visitHeader(header);
    }

    /*
    Simple way to extract a substring of the format
    "prefixSomeDataToExtractSuffix"
    Computes the end of the prefix and end of the data to extract (based on length of source and suffix)
    and uses String.substring to grab the data
    Done in place of dealing with a scanner
     */
    private static String extractString(String src, String prefix, String suffix){
        int srcLen = src.length();
        int start = prefix.length();
        int end = srcLen - suffix.length();
        if(start >= srcLen || end > srcLen || start > end)
            return ""; //cases that would otherwise throw StringIndexOutOfBoundsException
        return src.substring(start, end);
    }

    public static int getPoolId(Path p){
        try {
            return Integer.parseInt(extractString(p.getParent().getFileName().toString(), "pool-", ""));
        } catch (NumberFormatException _){
            return -1;
        }
    }

    public static int getTaskStamp(Path p){
        try {
            return Integer.parseInt(extractString(p.getFileName().toString(), "recording.worker", ".dat"));
        } catch (NumberFormatException _){
            return -1;
        }
    }

    /*
    Process a region of primitive data from optimized events
    Several optimized events may be back to back
    This method detects the boundary between them and executes a callback to taskVisitor.visitOptimized for each one

    Q: Why don't we parse the optimized regions here and call more specific callbacks?
    A: This implementation is "more powerful".
       We can create a TaskVisitor subclass that invokes more specific callbacks from this implementation.
       Doing the callbacks right away makes it more difficult to have a "catch all" that can visit log files even
       as JMVX changes.

     Some events don't write a size.
     ZERO_SIZE is a lookup map that allows us to determine when to change parsing strategy.
     Note: change to a bitmap in OptimizedEvent?
     */
    private static Set<OptimizedEvent> ZERO_SIZE = new TreeSet<>(Arrays.asList(
            OptimizedEvent.FAST_MONITOR_ENTER,
            OptimizedEvent.FAST_WAIT,
            OptimizedEvent.SINGLE_THREADED,
            OptimizedEvent.WRITE_NO_VALIDATION,
            OptimizedEvent.EXCEPTION
    ));

    public static void processOptimizedRegion(ObjectInputStream stream, TaskVisitor taskVisitor, int bytes) throws IOException {
        while(bytes > 0){
            OptimizedEvent tag = OptimizedEvent.readTag(stream);
            int size = ZERO_SIZE.contains(tag) ? 0 : stream.readInt();
            /* Skip EXCEPTION tags because a serialized object should follow
            * Realistically, we could return from this method as soon as we see this tag */
            if(!tag.equals(OptimizedEvent.EXCEPTION)) {
                /*
                TODO: optimize to reuse the buffer
                 */
                byte[] buffer = new byte[size];
                stream.readFully(buffer);
                /*
                Data stream is going to be a lot of packing and unpacking
                 */
                DataInputStream content = new DataInputStream(new ByteArrayInputStream(buffer));
                taskVisitor.visitOptimized(tag, size, content); //up to them to decode based on tag
                content.close();
            }
            bytes -= Integer.BYTES //tag
                    + Integer.BYTES  //size
                    + size; //content
        }
    }

    public static void processTask(ObjectInputStream stream, TaskVisitor taskVisitor) {
        //read data and call corresponding visitMethods methods
        try {
            readHeader(stream, taskVisitor);
            //read event stream until we run out
            while(true){
                try {
                    //serialized object encountered
                    /*
                    Oddity: can you serialize a byte array as an object?
                    It looks like it from some output I've seen
                     */
                    Object o = stream.readObject();
                    taskVisitor.visit(o);
                } catch (EOFException _){
                    break;
                } catch (OptionalDataException e){
                    //raw bytes encountered from an optimized event
                    processOptimizedRegion(stream, taskVisitor, e.length);
                } catch (ClassNotFoundException e) {
                    System.err.println("Err: " + e.getMessage() + " skipping event and continuing");
                    //e.printStackTrace();
                }
            }
        } catch (EOFException _) {
            //stream is empty? pass
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException exception) {
            System.err.println("Unusual error, message:");
            exception.printStackTrace();
            System.err.println("Skipping...");
        }
        taskVisitor.visitEnd();
    }

    public static void processTask(Path toTask, TaskVisitor taskVisitor) {
        //read data and call corresponding visitMethods methods
        try(ObjectInputStream stream = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(toTask.toString())))) {
            /*This is an odd spot for this code, but this is the last reference we have
            to the path used to create the task visitor
            So we optionally visit the pool info here if we are able to parse it
             */
            int poolId = getPoolId(toTask);
            int stamp = getTaskStamp(toTask);
            if(poolId >= 0 && stamp >= 0)
                taskVisitor.visitPoolInfo(poolId, stamp);
            processTask(stream, taskVisitor);
        } catch (IOException e) {
            /*mostly for FileNotFoundException, made more general for type checking
            ObjectInputStream can throw a general IOException if it fails to read data in the header (I guess)
            processTask(stream, task visitor) will cover IOExecptions during processing
            So if we ever hit here, we never processed the stream to begin with*/
            e.printStackTrace();
            taskVisitor.visitEnd();
        }
    }

    public static void processTask(byte[] task, TaskVisitor taskVisitor){
        //read data and call corresponding visitMethods methods
        try(ObjectInputStream stream = new ObjectInputStream(
                new BufferedInputStream(new ByteArrayInputStream(task)))) {
            processTask(stream, taskVisitor);
        } catch (IOException e) {
            /*See comment in processTask(Path, TaskVisitor)*/
            e.printStackTrace();
            taskVisitor.visitEnd();
        }
    }

    public static void processLoader(Path toLoader, LoaderVisitor loaderVisitor){
        //read data and call corresponding visitMethods methods
        try(ObjectInputStream stream = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(toLoader.toString())))) {
            while(true){
                /*
                NOTE: do not parallelize the block below
                The order of RRWriter's LoadTaskWriter will write to a log file
                as it processes it.
                While reading can be parallelized, it will compromise the writer.
                 */
                try {
                    String loaderClassName = (String) stream.readObject();
                    String className = (String) stream.readObject();
                    byte[] bytes = (byte[]) stream.readObject();
                    loaderVisitor.visitLoadTask(loaderClassName, className).ifPresent(
                            (v) -> processTask(bytes, v));
                } catch (EOFException _) {
                    break;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (ClassCastException e){
                    System.err.println("Failed to parse entry in class loading log, err:");
                    e.printStackTrace();
                    System.err.println("Attempting to skip and continue processing...");
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException exception) {
            System.err.println("Unusual error in " + toLoader + ", skipping");
            System.err.println("Error message:");
            exception.printStackTrace();
        }
        loaderVisitor.visitEnd();
    }

    public static void processClinits(Path toClinits, ClinitVisitor clinitVisitor){
        //read file and give details to clinitVisitor
        try(ObjectInputStream stream = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(toClinits.toString())))) {
            while(true){
                /*
                NOTE: do not parallelize the block below
                While reading can be parallelized, it will compromise the writer.
                See comment in processLoader, it's the same reason.
                 */
                try {
                    String className = (String) stream.readObject();
                    byte[] bytes = (byte[]) stream.readObject();
                    clinitVisitor.visitClinitTask(className).ifPresent((v) ->
                            processTask(bytes, v));
                } catch (EOFException _) {
                    break;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (ClassCastException e){
                    System.err.println("Failed to parse entry in class loading log, err:");
                    e.printStackTrace();
                    System.err.println("Attempting to skip and continue processing...");
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException exception) {
            System.err.println("Unusual error in " + toClinits + ", skipping");
            System.err.println("Error message:");
            exception.printStackTrace();
        }
        clinitVisitor.visitEnd();
    }

    /* Modified from code snippet in javadoc */
    private void shutdownAndAwaitTermination() {
        if(threadPool.isTerminated())
            return;

        threadPool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            threadPool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private void collectAllFilePaths(Collection<Path> collector, Path path){
        Stack<Path> dirs = new Stack<>();
        dirs.push(path);
        do {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirs.pop())) {
                stream.forEach((p) -> {
                    if (p.toFile().isDirectory())
                        dirs.push(p);
                    else
                        collector.add(p);
                });
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }while(!dirs.isEmpty());
    }

    @Override
    public void accept(RRVisitor visitor) {
        /*
        Visitors are optionals and an empty tells us to skip reading the file entirely.
         */
        Queue<Future<?>> futures = new LinkedBlockingQueue<>(); //just use a completion service lad
        Optional<Future<?>> maybeFuture;
        List<Path> filePaths = new LinkedList<>();
        collectAllFilePaths(filePaths, toRecording);
        for(Path entry : filePaths){ //can we parallelize here?
            Path filename = entry.getFileName();
            if(filename.equals(Paths.get(LOADER_DAT))){
                //visitor.visitLoader().ifPresent(
                //        (v) -> threadPool.execute(() -> processLoader(entry, v)));
                maybeFuture = visitor.visitLoader().map((v) -> threadPool.submit(() -> processLoader(entry, v)));
            }else if(filename.equals(Paths.get(CLINITS_DAT))) {
                //visitor.visitClinits().ifPresent(
                //        (v) -> threadPool.execute(() -> processClinits(entry, v)));
                maybeFuture = visitor.visitClinits().map((v) -> threadPool.submit(() -> processClinits(entry, v)));
            }else if(filename.toString().endsWith(".dat") && !NOT_TASK_FILES.contains(filename)){
                //visitor.visitTask(entry).ifPresent(
                //        (v) -> threadPool.execute(() -> processTask(entry, v)));
                maybeFuture = visitor.visitTask(entry).map((v) -> threadPool.submit(() -> processTask(entry, v)));
            }else{
                //other file in the directory, eg .jar file logged by a class loader
                //skipping for now, but will add code to process later
                maybeFuture = visitor.visitOther(entry).map((callable) -> threadPool.submit(callable));
            }
            maybeFuture.ifPresent(futures::add);
        }

        for(Future f = futures.poll(); f != null; f = futures.poll()){
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        shutdownAndAwaitTermination(); //doing this for now, I want to remove this at some point
    }

    @Override
    protected void finalize() throws Throwable {
        shutdownAndAwaitTermination();
        super.finalize();
    }
}
