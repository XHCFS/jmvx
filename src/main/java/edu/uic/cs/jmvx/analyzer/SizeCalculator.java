package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Calculates the size of tagged union reads/writes/monitor events (includes random access)
 * and breaks it down into 3 categories: size of data, size of overhead (eg tags/args), and total
 * Should be updated to look into some serialized events that read data from zipfiles
 *
 * Note: this analysis deals with tagged unions in optimized writes (eg avoidSerialization = true)
 * and will not return correct data when that feature is off.
 */
public class SizeCalculator extends RRVisitor {

    AtomicLong globalBytesRead = new AtomicLong(0);
    AtomicLong globalOverheadForReads = new AtomicLong(0);
    AtomicLong globalBytesWritten = new AtomicLong(0);
    AtomicLong globalBytesFromSlowClocks = new AtomicLong(0);
    AtomicLong globalOverheadForSlowClocks = new AtomicLong(0);
    AtomicLong globalBytesFromFastClocks = new AtomicLong(0); //I'm curious

    public SizeCalculator() {
        super(Optional.empty());
    }

    public SizeCalculator(Optional<RRVisitor> parent) {
        super(parent);
    }

    public class SizeVisitor extends TaskVisitor {

        long bytesRead = 0;
        long overheadForReads = 0;
        long totalBytesWritten = 0;
        long bytesFromSlowClocks = 0;
        long overheadForSlowClocks = 0;
        long bytesFromFastClocks = 0; //I'm curious

        public SizeVisitor(Optional<TaskVisitor> parent) {
            super(parent);
        }

        @Override
        public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
            super.visitOptimized(tag, size, content);
            try {
                switch (tag) {
                    case READ:
                    case RANDOM_ACCESS_READ:
                    case ELIDED_READ:
                    case ELIDED_RANDOM_ACCESS_READ:
                        int rOff = content.readInt();
                        int rLen = content.readInt();
                        int ret = content.readInt();
                        int read = (ret > 0? ret : 0);
                        bytesRead += read;
                        //bytes from logging tag and params
                        overheadForReads += OptimizedEvent.TAG_SIZE + size - ret;
                    break;
                    case WRITE:
                    case RANDOM_ACCESS_WRITE:
                    case ELIDED_WRITE:
                    case ELIDED_RANDOM_ACCESS_WRITE:
                        /* write format is not standardized
                         * sometimes we log arguments, sometimes we don't
                         * but they all use the same tag */
                        totalBytesWritten += OptimizedEvent.TAG_SIZE + size;
                        break;
                    case WRITE_NO_VALIDATION:
                        totalBytesWritten += OptimizedEvent.TAG_SIZE; //is no payload
                        break;
                    case MONITOR_ENTER:
                    case WAIT:
                        bytesFromSlowClocks += size;
                        overheadForSlowClocks += OptimizedEvent.TAG_SIZE;
                        break;
                    case FAST_MONITOR_ENTER:
                    case FAST_WAIT:
                        bytesFromFastClocks += OptimizedEvent.TAG_SIZE;
                        break;
                    default:
                        break;
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        @Override
        public void visitEnd() {
            //update the global count
            globalBytesRead.addAndGet(bytesRead);
            globalOverheadForReads.addAndGet(overheadForReads);
            globalBytesWritten.addAndGet(totalBytesWritten);
            globalBytesFromSlowClocks.addAndGet(bytesFromSlowClocks);
            globalOverheadForSlowClocks.addAndGet(bytesFromSlowClocks);
            globalBytesFromFastClocks.addAndGet(bytesFromFastClocks);
            super.visitEnd();
        }
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        return Optional.of(new SizeVisitor(super.visitTask(toTask)));
    }

    @Override
    public Optional<LoaderVisitor> visitLoader() {
        return Optional.of(new LoaderVisitor(super.visitLoader()){
            @Override
            public Optional<TaskVisitor> visitLoadTask(String loaderClassName, String className) {
                return Optional.of(new SizeVisitor(super.visitLoadTask(loaderClassName, className)));
            }
        });
    }

    @Override
    public Optional<ClinitVisitor> visitClinits() {
        return Optional.of(new ClinitVisitor(super.visitClinits()){
            @Override
            public Optional<TaskVisitor> visitClinitTask(String className) {
                return Optional.of(new SizeVisitor(super.visitClinitTask(className)));
            }
        });
    }

    public double toMB(long bytes){
        return (double) bytes / 1024 / 1024;
    }

    public void printStats(){
        double bytesRead = toMB(globalBytesRead.get());
        double overheadForReads = toMB(globalOverheadForReads.get());
        double bytesWritten = Double.NaN; //can't compute yet
        double overheadForWrites = Double.NaN; //can't compute yet
        double totalForWrites = toMB(globalBytesWritten.get());
        double bytesFromSlowClocks = toMB(globalBytesFromSlowClocks.get());
        double overheadForSlowClocks = toMB(globalOverheadForSlowClocks.get());
        double bytesFromFastClocks = toMB(globalBytesFromFastClocks.get());

        System.out.println("Optimized event data (in MB)");
        System.out.format("Read       : %.2f\tOverhead:%.2f\tTotal:%.2f\n",
                bytesRead, overheadForReads, bytesRead + overheadForReads);
        System.out.format("Write      : %.2f\tOverhead:%.2f\tTotal:%.2f\n",
                bytesWritten, overheadForWrites, totalForWrites);
        System.out.format("Slow clocks: %.2f\tOverhead:%.2f\tTotal:%.2f\n",
                bytesFromSlowClocks, overheadForSlowClocks, bytesFromSlowClocks + overheadForSlowClocks);
        //fast clock only logs a tag
        System.out.format("Fast clocks: %.2f\tOverhead:%.2f\tTotal:%.2f\n",
                0.0, bytesFromFastClocks, bytesFromFastClocks);
    }

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Please supply the directory to a replay");
        }else{
            Path toRec = Paths.get(args[0]);
            RRReader reader = new RRReader(toRec);
            SizeCalculator visitor = new SizeCalculator();
            reader.accept(visitor);

            visitor.printStats();
        }
    }
}
