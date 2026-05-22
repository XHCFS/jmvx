package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.analyzer.util.InfiniteSingletonList;
import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.strategy.Leader;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Depracated class because we do this online now.
 * Insert UIDedObject as the parent of all classes whose parent is Object (unless they are in java.lang)
 * UIDedObject stores a reference to the last owner, which allows us to compute partial order during runtime.
 *
 * Note: this class will not produce sensible results currently.
 * The MonitorEnter and Wait no longer have the hashcode fields this class relied on
 */
@Deprecated
public class PartialOrderAnalyzer extends RRVisitor {

    private boolean canAvoidVC = false; //set in the summarize method

    private static class SyncOp {
        public final long globalSeq;
        public final int hashcode;
        public final Path path;
        /*in place modification saves about half a second
         versus creating and returning different lists
         when analyzing pmd's recording*/
        public volatile boolean partiallyOrderable = false; //going to be set in analysis.

        public SyncOp(long globalSeq, int hashcode, Path path) {
            this.globalSeq = globalSeq;
            this.hashcode = hashcode;
            this.path = path;
        }
    }

    /*
    Maintain 2 different mappings of monitor events
    taskSyncOps   - logfile -> List<SyncOps>
    opsByHashCode - object hashcode -> List<SyncOps>

    The former is used by the writer to determine which events to rewrite
    The latter is used to determine which events are partially orderable
    IMPORTANT NOTES:
    The List<SyncOps> are not thread safe data structures!
    You need to synchronize on the list before using it
    The SyncOp objects inside the lists are shared between the maps

    The analyzer has three phases
    1. reading   - populates both maps
    2. analyzing - uses opsByHashCode to MODIFY A VOLATILE FIELD (op.partiallyOrderable) in each SyncOp
    3. writing   - uses taskSyncOps to modify the log, based on the value of op.partiallyOrderable
     */
    ConcurrentHashMap<Path, List<SyncOp>> taskSyncOps = new ConcurrentHashMap();
    ConcurrentHashMap<Integer, List<SyncOp>> opsByHashCode = new ConcurrentHashMap<>();

    //unused
    public <K, V> V getOrDefaultLazy(Map<K, V> map, K key, Supplier<V> lazyGenerator){
        V value = map.get(key);
        if(value == null)
            value = lazyGenerator.get();
        return value;
    }

    /*
    Works a little differently than putIfAbsent
    Returns the current value, including one we may have put there
     */
    public <K, V> V putIfAbsentLazy(Map<K, V> map, K key, Supplier<V> lazyGenerator){
        /* needs to be atomic otherwise, 2 threads may generate and add 2 values at the same time
        * Note: remove the locking from this method and have the caller lock?
        *       Some callers work on local data, eliminating the race condition. */
        synchronized (map) {
            V value = map.get(key);
            if (value == null) {
                value = lazyGenerator.get();
                map.put(key, value);
            }
            return value;
        }
    }

    //array list b/c we need to sort eventually
    //and sorting an array list is faster
    Supplier<List<SyncOp>> newListSupplier = () -> new ArrayList<SyncOp>();

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        Optional<TaskVisitor> parent = super.visitTask(toTask);
        return Optional.of(new TaskVisitor(parent) {

            /*
            Task visitor is called on any given thread,
            so we use local collections for easy data population
            (until visitEnd, where drain our local collections into the "globals")
             */
            List<SyncOp> syncOps = new ArrayList<>();
            HashMap<Integer, List<SyncOp>> byHashCode = new HashMap<>();

            @Override
            public void visit(Object o) {
                super.visit(o);
                int hashcode;
                long[] clock;
                if (o instanceof Leader.MonitorEnter){
                    Leader.MonitorEnter record = (Leader.MonitorEnter) o;
                    hashcode = 0;//record.hashcode; field was removed
                    clock = record.clock;
                }else if (o instanceof Leader.Wait){
                    Leader.Wait record = (Leader.Wait) o;
                    hashcode = record.hashcode;
                    clock = record.clock;
                }else{
                    return;
                }
                addNewSyncOp(hashcode, clock);
            }

            private void addNewSyncOp(int hashcode, long[] clock) {
                SyncOp op = new SyncOp(Arrays.stream(clock).sum(), hashcode, toTask);
                //local version, no lock needed
                putIfAbsentLazy(byHashCode, hashcode, newListSupplier).add(op);
                syncOps.add(op);
            }

            @Override
            public void visitByteArray(byte[] arr) {
                super.visitByteArray(arr);
                /*
                Issue is this visitor does not separate events.
                Eg if 3 monitor enters are back to back,
                all of them are passed via the same byte array
                RRReader needs to process this data block a bit more.
                Eg visitOptimized(OptimizedEvent tag, DataInput content)
                Or we write custom visitor for this in RRReader
                Eg visitOptimizedMonitorEnter, visitOptimizedWait, eg
                That way, each analysis does not have to reimplement parsing logic
                 */
                try(DataInputStream hack = new DataInputStream(new ByteArrayInputStream(arr))){
                    OptimizedEvent tag = OptimizedEvent.readTag(hack);
                    if(isSyncOp(tag)){
                        int hashcode = hack.readInt();
                        long[] clock = decodeClock(hack, arr.length - Integer.BYTES);
                        addNewSyncOp(hashcode, clock);
                    } //else pass
                } catch (IOException exception) {
                    exception.printStackTrace();
                }
            }

            @Override
            public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
                super.visitOptimized(tag, size, content);
                if(isSyncOp(tag)){
                    try {
                        int hashcode = content.readInt();
                        long[] clock = decodeClock(content, size - Integer.BYTES);
                        addNewSyncOp(hashcode, clock);
                    }catch (IOException e){
                        throw new Error("Failed to process vector clock", e);
                    }
                }
            }

            @Override
            public void visitEnd() {
                //add our locally counted sync ops to the program total, which will be sorted later
                byHashCode.forEach((hashcode, opList) -> {
                    List<SyncOp> globalListForHash = putIfAbsentLazy(opsByHashCode, hashcode, newListSupplier);
                    synchronized (globalListForHash){
                        globalListForHash.addAll(opList);
                    }
                });
                taskSyncOps.put(toTask, syncOps);
                super.visitEnd();
            }
        });
    }

    public static final long[] NOCLOCK = new long[0];

    public static long[] decodeClock(DataInput content, int clockBytes) throws IOException {
        //int length = content.readByte();
        //if(length == 0)
        //    return NOCLOCK;
        long[] clock = new long[clockBytes / Long.BYTES];
        /*reads in wrong endianness
        for (int i = 0; i < length; i++) {
            clock[i] = content.readLong();
        }*/
        byte[] bytes = new byte[clockBytes];
        content.readFully(bytes);
        JMVXRuntime.unsafe.copyMemory(bytes, CircularBuffer.byteOffset, clock, CircularBuffer.longOffset, bytes.length);
        return clock;
    }

    private byte[] encodeClock(long[] clock) throws IOException {
        int length = clock.length;
        byte[] bytes = new byte[Long.BYTES * length];
        JMVXRuntime.unsafe.copyMemory(clock, CircularBuffer.longOffset, bytes, CircularBuffer.byteOffset, bytes.length);
        return bytes;
    }

    /*Process based on hashcodes themselves, rather than log files.
    1 task per hashcode
    Hashcode -> List<SynOp>
    Then we can easily process partial ordering
    If lastOwner = unowned or lastOwner = nextOwner
      partial order
    else
      total order
    */
    /* Memory isolated implementation.
    Instead of inplace modification, create new maps with parially orderable events
    public Map<Path, List<SyncOp>> findPartiallyOrderablePerTask(){
        //have to make a pool and execute task for each
        List<SyncOp> partiallyOrderable = new ArrayList<>();
        opsByHashCode.forEachKey(30, (hashcode) -> {
            List<SyncOp> events = findPartiallyOrderableForHash(hashcode);
            synchronized (partiallyOrderable){
                partiallyOrderable.addAll(events);
            }
        });
        //reorganize to be per task (not per hashcode)
        HashMap<Path, List<SyncOp>> byTask = new HashMap<>();
        for(SyncOp op: partiallyOrderable){
            putIfAbsentLazy(byTask, op.path, newListSupplier).add(op);
        }
        //should all be for the same file now
        byTask.forEach((path, ops) -> ops.sort(Comparator.comparingLong((o) -> o.globalSeq)));
        return byTask;
    }

    public List<SyncOp> findPartiallyOrderableForHash(Integer hashcode){
        List<SyncOp> ops = opsByHashCode.get(hashcode);
        synchronized (ops) {
            if (ops.size() == 0)
                return new ArrayList<>(); //nothing partially orderable
            List<SyncOp> partiallyOrderable = new ArrayList<>(); //maybe get away with linked list here
            ops.sort(Comparator.comparingLong((op) -> op.globalSeq));
            //first use is partially orderable
            Iterator<SyncOp> iter = ops.iterator();
            SyncOp lastEvent = iter.next();
            partiallyOrderable.add(lastEvent);
            while (iter.hasNext()) {
                SyncOp nextEvent = iter.next();
                //same owner, partially orderable
                if (nextEvent.path.equals(lastEvent.path))
                    partiallyOrderable.add(nextEvent);
                lastEvent = nextEvent;
            }
            return partiallyOrderable;
        }
    }

    public void summarize(){
        Map<Path, List<SyncOp>> partiallyOrderableByTask = findPartiallyOrderablePerTask();
        synchronized (taskSyncOps){
            taskSyncOps.entrySet().stream().forEach((entry) -> {
                System.out.format("%s %d / %d\n", entry.getKey(),
                        partiallyOrderableByTask.get(entry.getKey()).size(),
                        entry.getValue().size());
            });
        }
    }*/

    public void markPartiallyOrderableForHashInPlace(Integer hashcode){
        List<SyncOp> ops = opsByHashCode.get(hashcode);
        synchronized (ops) {
            if (ops.size() == 0)
                return; //nothing partially orderable
            ops.sort(Comparator.comparingLong((op) -> op.globalSeq));
            //first use is partially orderable
            Iterator<SyncOp> iter = ops.iterator();
            SyncOp lastEvent = iter.next();
            lastEvent.partiallyOrderable = true;
            while (iter.hasNext()) {
                SyncOp nextEvent = iter.next();
                //same owner, partially orderable
                if (nextEvent.path.equals(lastEvent.path))
                    nextEvent.partiallyOrderable = true;
                lastEvent = nextEvent;
            }
        }
    }

    /*
    Sets canAvoidVC to represent if we can avoid the vector clock
    true -> effectively single threaded
    false -> at least 1 thread has a totally ordered operation.

    The detection of avoiding the vc and printing the results probably should be split up
    but this saves us on some iterations over the data
     */
    public void summarize(){
        opsByHashCode.forEachKey(4, (hashcode) -> markPartiallyOrderableForHashInPlace(hashcode));
        synchronized (taskSyncOps){
            canAvoidVC = taskSyncOps.entrySet().stream().mapToLong((entry) -> {
                long numOptimizable = entry.getValue().stream().filter((e) -> e.partiallyOrderable).count();
                long numOps = entry.getValue().size();
                System.out.format("%s %d / %d\n", entry.getKey(),
                        numOptimizable,
                        numOps);
                return numOps - numOptimizable;
            }).sum() == 0L; //don't short circuit, because this also prints out the report!
        }

        if(canAvoidVC)
            System.out.println("Effectively single threaded, rewriting to avoid vector clock");
    }

    public ConcurrentHashMap<Path, List<SyncOp>> getTaskSyncOps() {
        return taskSyncOps;
    }

    public static class PartialOrderWriter extends RRVisitor{
        final ConcurrentHashMap<Path, List<SyncOp>> taskSyncOps;
        final boolean avoidVC;

        public PartialOrderWriter(Optional<RRVisitor> parent, ConcurrentHashMap<Path, List<SyncOp>> taskSyncOps,
                                  boolean avoidVC) {
            super(parent);
            this.taskSyncOps = taskSyncOps;
            this.avoidVC = avoidVC;
        }

        public PartialOrderWriter(Optional<RRVisitor> parent, PartialOrderAnalyzer analysis){
            this(parent, analysis.getTaskSyncOps(), analysis.canAvoidVC);
        }

        @Override
        public Optional<TaskVisitor> visitTask(Path toTask) {
            return Optional.of(new SyncOpReWriter(super.visitTask(toTask), taskSyncOps.get(toTask), avoidVC));
        }

        public TaskVisitor avoidVCVisitor(Optional<TaskVisitor> parent){
            SyncOp s = new SyncOp(0, 0, Paths.get(""));
            s.partiallyOrderable = true;
            return new SyncOpReWriter(parent, new InfiniteSingletonList<>(s), true);
        }

        /*
        Idea: if we determine to completely avoid the vc, then we also avoid the vc for clinits and loading
        This may cause a lot of issues, but it is worth a shot.
         */

        @Override
        public Optional<LoaderVisitor> visitLoader() {
            return Optional.of(new LoaderVisitor(super.visitLoader()){
                @Override
                public Optional<TaskVisitor> visitLoadTask(String loaderClassName, String className) {
                    if(avoidVC){
                        return Optional.of(avoidVCVisitor(super.visitLoadTask(loaderClassName, className)));
                    }else {
                        return super.visitLoadTask(loaderClassName, className);
                    }
                }
            });
        }

        @Override
        public Optional<ClinitVisitor> visitClinits() {
            return Optional.of(new ClinitVisitor(super.visitClinits()){
                @Override
                public Optional<TaskVisitor> visitClinitTask(String className) {
                    if(avoidVC){
                        return Optional.of(avoidVCVisitor(super.visitClinitTask(className)));
                    }else {
                        return super.visitClinitTask(className);
                    }
                }
            });
        }
    }

    public static class SyncOpReWriter extends TaskVisitor {
        final List<SyncOp> syncOps;
        Iterator<SyncOp> iter;
        final boolean avoidVC;

        public SyncOpReWriter(Optional<TaskVisitor> parent, List<SyncOp> syncOps, boolean avoidVC) {
            super(parent);
            this.syncOps = syncOps;
            this.iter = this.syncOps.iterator();
            this.avoidVC = avoidVC;
        }

        /*
        Create a new MonitorEnter or Wait object that is partially orderable
        For referential transparency, if rewrite is given a different class of object, it returns the identity.
        For now, the clock and hashcode are left in the rewritten object.
         */
        public Object rewrite(Object o){
            if (o instanceof Leader.MonitorEnter){
                Leader.MonitorEnter record = (Leader.MonitorEnter) o;
                //return new Leader.MonitorEnter(record.clock, record.hashcode, true);
                return new Leader.MonitorEnter(null, //record.hashcode, field was removed
                        avoidVC ? OptimizedEvent.SINGLE_THREADED : OptimizedEvent.FAST_MONITOR_ENTER);
            }else if (o instanceof Leader.Wait){
                Leader.Wait record = (Leader.Wait) o;
                return new Leader.Wait(null, record.hashcode,
                        avoidVC ? OptimizedEvent.SINGLE_THREADED : OptimizedEvent.FAST_WAIT);
            }else {
                //cannot rewrite any other entries
                return o;
            }
        }

        @Override
        public void visit(Object o) {
            if (isSyncOp(o)){
                if(!iter.hasNext()) {
                    /*
                    Shouldn't happen
                    syncOps was creating by reading all syncOps in this file earlier
                    entries we read should match 1 to 1 with entries in list
                     */
                    throw new RuntimeException("Different number of sync ops seen by partial order writer");
                }else {
                    //rewrite the entry if it is partially orderable
                    Object toWrite = iter.next().partiallyOrderable ? rewrite(o) : o;
                    super.visit(toWrite);
                }
            }else{ //not a sync op
                super.visit(o);
            }
        }

        @Override
        public void visitByteArray(byte[] arr) {
            try(DataInputStream hack = new DataInputStream(new ByteArrayInputStream(arr))){
                OptimizedEvent tag = OptimizedEvent.readTag(hack);
                if(isSyncOp(tag)){
                    if(!iter.hasNext()){
                        throw new RuntimeException("Different number of sync ops seen by partial order writer");
                    }else{
                        SyncOp next = iter.next();
                        if(next.partiallyOrderable) {
                            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                            DataOutputStream out = new DataOutputStream(bytesOut);

                            int hashcode = hack.readInt();
                            out.write(hashcode);
                            int length = 0;//hack.readByte();
                            out.writeByte(length);
                            out.close();
                            //write the optimized form back
                            super.visitByteArray(bytesOut.toByteArray());
                        }else{
                            //not partially orderable, write the same bytes back
                            super.visitByteArray(arr);
                        }
                    }
                } else {
                    //write the array back
                    super.visitByteArray(arr);
                }
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }


        public OptimizedEvent rewriteTag(OptimizedEvent tag){
            if(tag.equals(OptimizedEvent.MONITOR_ENTER))
                return avoidVC ? OptimizedEvent.SINGLE_THREADED : OptimizedEvent.FAST_MONITOR_ENTER;
            else if(tag.equals(OptimizedEvent.WAIT))
                return avoidVC ? OptimizedEvent.SINGLE_THREADED : OptimizedEvent.FAST_WAIT;
            else
                return tag;
        }
        /*
        Passing a stream may be a bad idea
        because the stream is stateful
        so if I read a primitive here and then call super, it won't be readable there
        This is a problem.
         */
        @Override
        public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
            if(isSyncOp(tag)){
                if(!iter.hasNext()){
                    throw new RuntimeException("Different number of sync ops seen by partial order writer");
                }else {
                    SyncOp next = iter.next();
                    if (next.partiallyOrderable) {
                        byte[] buf = new byte[0];//bytesOut.toByteArray();
                        super.visitOptimized(rewriteTag(tag), 0, new DataInputStream(new ByteArrayInputStream(buf)));
                    } else {
                        //not partially orderable, write the same bytes back
                        super.visitOptimized(tag, size, content);
                    }
                }
            } else {
                //write the array back
                super.visitOptimized(tag, size, content);
            }
        }
    }

    private static boolean isSyncOp(Object o) {
        return o instanceof Leader.MonitorEnter || o instanceof Leader.Wait;
    }

    private static boolean isSyncOp(OptimizedEvent tag) {
        //hack to test something
        return tag.equals(OptimizedEvent.MONITOR_ENTER) || tag.equals(OptimizedEvent.WAIT);
    }

    public static void main(String[] args){
        if(args.length != 2){
            System.err.println("Usage [source recording directory] [destination directory for processed copy]");
        }else{
            Path toRec = Paths.get(args[0]);
            Path toCopy = Paths.get(args[1]);
            RRReader reader = new RRReader(toRec);
            PartialOrderAnalyzer summarizer = new PartialOrderAnalyzer();
            reader.accept(summarizer);

            summarizer.summarize();
            System.out.format("Rewriting to %s ...\n", toCopy);

            RRReader reader2 = new RRReader(toRec);
            RRWriter writer = new RRWriter(toCopy);
            PartialOrderWriter rewriter = new PartialOrderWriter(Optional.of(writer), summarizer);
            reader2.accept(rewriter);

            System.out.println("Done!");
        }
    }
}
