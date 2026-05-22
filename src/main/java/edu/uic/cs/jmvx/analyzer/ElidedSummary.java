package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.strategy.Recorder;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Computes counts of events Elided by the FollowerRecorder in HybridMVX/RR
 *
 * Some notes on behavior:
 * ELIDE_READ/WRITE means a read/write that the leader did not transmit to the follower was logged
 * e.g., by rereading the file on the follower's side
 * Non elided reads (eg READ) and similar events can still be in the elided summary
 * if they occur in class loading or clinits.
 */
public class ElidedSummary extends RRVisitor {

    protected Summary regularEvents = new Summary();
    protected Summary elidedEvents = new Summary();

    public ElidedSummary() {
        super(Optional.empty());
    }

    public ElidedSummary(Optional<RRVisitor> parent) {
        super(parent);
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        final TaskVisitor regularVisitor = regularEvents.visitTask(toTask).get();
        final TaskVisitor elidedVisitor = elidedEvents.visitTask(toTask).get();
        return Optional.of(new TaskVisitor(super.visitTask(toTask)){

            /**
             * LeaderRecorder does not do anything to zipfiles
             * Luckily, all of those events are defined in Recorder,
             * so we can filter out the events to throw to the elidedVisitor if the enclosing class is Recorder
             *
             * NOTE: if the log file only contains serialized events, this count will be incorrect!
             * I'm assuming all elided events during regular execution are marked as elided
             * TODO: add class Elided(Object o) extends Serializable event to mark serialized data as elided
             * @param o
             */
            @Override
            public void visit(Object o) {
                Optional<Class<?>> enclosing = Optional.ofNullable(o.getClass().getEnclosingClass());
                if(enclosing.map(c -> c.equals(Recorder.class)).orElse(false)){
                    elidedVisitor.visit(o);
                }else {
                    regularVisitor.visit(o);
                }
                super.visit(o);
            }

            /**
             * Move events with ELIDED tags into elidedVisitor/elidedEvents
             * Others go to regularVisitor/regularEvents
             * @param tag
             * @param size
             * @param content
             */
            @Override
            public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
                switch (tag){
                    case ELIDED_READ:
                    case ELIDED_WRITE:
                    case ELIDED_RANDOM_ACCESS_READ:
                    case ELIDED_RANDOM_ACCESS_WRITE:
                    case ELIDED_AVAILABLE:
                        elidedVisitor.visitOptimized(tag, size, content);
                        break;
                    default:
                        regularVisitor.visitOptimized(tag, size, content);
                }
                super.visitOptimized(tag, size, content);
            }

            /**
             * Need to call visitEnd of the inner visitors to correctly accumulate counts
             */
            @Override
            public void visitEnd() {
                regularVisitor.visitEnd();
                elidedVisitor.visitEnd();
                super.visitEnd();
            }
        });
    }

    /**
     * All events goto elided, LeaderRecorder does not deal with class loading
     * @return
     */
    @Override
    public Optional<LoaderVisitor> visitLoader() {
        return elidedEvents.visitLoader();
    }

    /**
     * All events goto elided, LeaderRecorder does not deal with clinits
     * @return
     */
    @Override
    public Optional<ClinitVisitor> visitClinits() {
        return elidedEvents.visitClinits();
    }

    /**
     * Computes the total number of events stored by a count map
     * @param counts Map of String -> Long, storing counts of events
     * @return the total number of events (sum of all longs in the map)
     */
    public static long eventCount(Map<String, Long> counts){
        return counts.entrySet().stream().mapToLong(Map.Entry::getValue).sum();
    }

    /**
     * Note: proportions of events are computed with respect to category, not total recording
     * Eg Regular READ with 0.5 means half of REGULAR events were reads, not half of ALL events.
     */
    public void report(){
        long regularTotal = eventCount(regularEvents.count);
        long elidedTotal = eventCount(elidedEvents.count);
        long total = regularTotal + elidedTotal;

        System.out.format(">>REGULAR EVENTS=%d\t\t%f\n", regularTotal, (double) regularTotal / (double) total);
        Summary.displaySummary(regularEvents.count, System.out);
        System.out.println();
        System.out.format(">>ELIDED EVENTS=%d\t\t%f\n", elidedTotal, (double) elidedTotal / (double) total);
        Summary.displaySummary(elidedEvents.count, System.out);
    }

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Please supply the directory to a replay");
        }else{
            Path toRec = Paths.get(args[0]);
            RRReader reader = new RRReader(toRec);
            ElidedSummary summarizer = new ElidedSummary();
            reader.accept(summarizer);

            summarizer.report();
        }
    }
}
