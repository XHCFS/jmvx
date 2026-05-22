package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Goal of this analysis is to filter events from dacapo into 2 categories:
 * 1) harness (before/after the timer starts)
 * 2) benchmark
 *
 * Identify this by looking for the write events which report benchmark starting/ending
 * Trick: [Optimized] Writes don't use a unified format right now
 * We cannot tell if the first few bytes belong to the arguments or the buffer
 * So we have to speculate a little bit.
 * 4 integers worth of bytes at a max will be non-buffer.
 * NOTE: this analysis does not work if avoiding object serialization
 * This is not a hard problem to fix, but we need to know which type of write object to look for data in
 * (and I haven't checked at the time of writing)
 *
 * The output of this analysis is:
 *
 * ===HARNESS===
 * `SizeCalculator` data
 * `ElidedSummary` data
 *
 * ===BENCHMARK===
 * `SizeCalculator` data
 * `ElidedSummary` data
 *
 * NOTE: the `SizeCalculator` data is computed over both regular and elided events!
 * If you want a split of that data, please modify the ElidedSummary into something like ElidedSizes.
 *
 * This analyzer DOES NOT look at class loading or clinits because we log them for random access lookup later.
 * In other words, we cannot tell if they happened before or after the benchmark
 * Albeit, we could guess based on the class loader and class name.
 * Omitting these events causes results to differ from running `SizeCalculator` and `ElidedSummary` separately.
 */
public class DacapoRunningAnalyzer extends RRVisitor {

    protected final RRVisitor harnessVisitor;
    protected final RRVisitor benchmarkVisitor;

    public DacapoRunningAnalyzer(RRVisitor harnessVisitor, RRVisitor benchmarkVisitor) {
        this.harnessVisitor = harnessVisitor;
        this.benchmarkVisitor = benchmarkVisitor;
    }

    public DacapoRunningAnalyzer(Optional<RRVisitor> parent, RRVisitor harnessVisitor, RRVisitor benchmarkVisitor) {
        super(parent);
        this.harnessVisitor = harnessVisitor;
        this.benchmarkVisitor = benchmarkVisitor;
    }

    private static boolean arrayEqualsWithOffset(byte[] as, int aOffset, byte[] bs, int bOffset, int length){
        //one of the arrays will reach its end before checking `length` bytes
        if(length > as.length - aOffset || length > bs.length - bOffset)
            return false;
        for(int i = 0; i < length; i++){
            if(as[aOffset + i] != bs[bOffset + i])
                return false;
        }
        return true;
    }

    protected class DacapoMessagePredicate extends EventPredicate {

        protected final byte[] message = "===== DaCapo".getBytes();
        protected final int MAX_ARGS_SPACE = Integer.BYTES * 3; //off, len, ret

        public DacapoMessagePredicate() {
            super(Optional.empty());
        }

        @Override
        public boolean predicate(Object o) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public boolean predicate(OptimizedEvent tag, int size, DataInput content) {
            switch (tag){
                case WRITE:
                case ELIDED_WRITE:
                    byte[] buf = new byte[Math.min(message.length + MAX_ARGS_SPACE, size)];
                    try {
                        content.readFully(buf);
                        for(int i = 0; i < MAX_ARGS_SPACE; i++){
                            if(arrayEqualsWithOffset(message, 0, buf, i, message.length))
                                return true;
                        }
                        return false;
                    }catch (IOException e){
                        throw new RuntimeException("Failed to read data", e);
                    }
                case WRITE_NO_VALIDATION:
                    //should we warn and return false?
                    throw new RuntimeException("Cannot detect dacapo start/end without validating writes");
                default:
                    return false;
            }
        }
    }

    /* NOTE: only the main thread runs the benchmark harness
     * For simplicity, all tasks outside of the main thread divert to the benchmarkVisitor.
     * So we do a check to see if we need to enable the toggle up front.
     * If we are in the "main" thread, then we toggle based on detecting dacapo start/end messages.
     */
    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        boolean enabled = toTask.toString().contains("main");
        if(enabled) {
            return Optional.of(new TaskVisitor(super.visitTask(toTask)) {
                final TaskVisitor harnessTaskVisitor = harnessVisitor.visitTask(toTask).get();
                final TaskVisitor benchmarkTaskVisitor = benchmarkVisitor.visitTask(toTask).get();
                //needs to be local so no other thread attempts to use it
                final EventPredicate dacapoStartEndPredicate = new DacapoMessagePredicate();
                boolean inHarness = true;

                //not overriding visit(Object o) because the DacapoMessagePredicate does not implement that method
                @Override
                public void visit(Object o) {
                    //TODO: add check for dacapoStartEndPredicate here.
                    // Not sure which write event dacapo uses (b[] or b[]II)
                    if(inHarness){
                        harnessTaskVisitor.visit(o);
                    }else{
                        benchmarkTaskVisitor.visit(o);
                    }
                    super.visit(o);
                }

                @Override
                public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
                    //check the predicate
                    dacapoStartEndPredicate.visitOptimized(tag, size, content);
                    if (dacapoStartEndPredicate.getResult()) {
                        inHarness = !inHarness; //flip the toggle
                    }

                    //check the value of the switch and visit accordingly
                    if(inHarness){
                        harnessTaskVisitor.visitOptimized(tag, size, content);
                    }else{
                        benchmarkTaskVisitor.visitOptimized(tag, size, content);
                    }

                    super.visitOptimized(tag, size, content);
                }

                @Override
                public void visitEnd() {
                    //propagate results
                    harnessTaskVisitor.visitEnd();
                    benchmarkTaskVisitor.visitEnd();
                    super.visitEnd();
                }
            });
        }else{
            return benchmarkVisitor.visitTask(toTask);
        }
    }

    /* not overriding loader or clinits because we can't tell when they happen
     * They are stored for random access later, therefore ordering info is not preserved
     */

    public static final String BORDER = "===========";

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Please supply the directory to a replay");
        }else{
            Path toRec = Paths.get(args[0]);
            RRReader reader = new RRReader(toRec);
            SizeCalculator harnessSize = new SizeCalculator();
            ElidedSummary harnessSummary = new ElidedSummary(Optional.of(harnessSize));
            SizeCalculator benchmarkSize = new SizeCalculator();
            ElidedSummary benchmarkSummary = new ElidedSummary(Optional.of(benchmarkSize));
            DacapoRunningAnalyzer summarizer = new DacapoRunningAnalyzer(harnessSummary, benchmarkSummary);
            reader.accept(summarizer);

            System.out.println("NOTICE: Skipping clinits and class loading events");
            System.out.format("%sHARNESS%s\n", BORDER, BORDER);
            System.out.println("Sizes of read/write/monitor events (both regular and elided):");
            harnessSize.printStats();
            System.out.println();
            harnessSummary.report();
            System.out.println();
            System.out.format("%sBENCHMARK%s\n", BORDER, BORDER);
            System.out.println("Sizes of read/write/monitor events (both regular and elided):");
            benchmarkSize.printStats();
            System.out.println();
            benchmarkSummary.report();
        }
    }
}
