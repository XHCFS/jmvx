package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.strategy.Leader;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;

/*
Idea is to summarize all events between monitor enters.
To allow someone to get the gist of what is happening in between

Output format
[Summary1]

[Summary2]
...
[SummaryN]

The newlines are purposeful and separate the summaries
All monitorenters in a summary are SEQUENTIAL and occur at the end of the event stream for that summary
All other events in the summary are UNORDERED
 */
public class TaskSummary extends RRVisitor{

    private static final Path summariesPath = Paths.get("summaries");

    static{
        summariesPath.toFile().mkdirs();
    }

    private boolean isTask(Path toTask){
        //need to convert to string
        return toTask.getFileName().toString().startsWith("recording.worker");
    }

    private Path summaryPath(Path toTask){
        return summariesPath.resolve(toTask.getFileName().toString().replace(".dat", ".txt"));
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        Optional<TaskVisitor> parentVisitor = super.visitTask(toTask);
        return isTask(toTask) ? Optional.of(new PartialSummarizer(parentVisitor, toTask)) : parentVisitor;
    }

    /*
    Creates a new EventCounter every time we encounter a MonitorEnter
     */
    private class PartialSummarizer extends TaskVisitor {
        LinkedList<EventCounter> summaries = new LinkedList<>();
        EventCounter currentCounter;
        Path toTask;
        int consecutiveEnters = 0;

        public PartialSummarizer(Optional<TaskVisitor> parent, Path toTask) {
            super(parent);
            this.toTask = toTask;
            currentCounter = new EventCounter(parent);
            summaries.add(currentCounter);
        }

        @Override
        public void visit(Object o) {
            if(o instanceof Leader.MonitorEnter) {
                consecutiveEnters++;
            }else if(consecutiveEnters > 0){
                //start a new summary
                currentCounter.visitEnd();
                currentCounter = new EventCounter(parent);
                summaries.add(currentCounter);
                consecutiveEnters = 0;
            }
            currentCounter.visit(o);
            super.visit(o);
        }

        @Override
        public void visitEnd() {
            currentCounter.visitEnd();
            Path newSummaryPath = summaryPath(toTask);
            //log summaries
            try(PrintStream out = new PrintStream(new FileOutputStream(newSummaryPath.toFile()))){
                Iterator<EventCounter> iter = summaries.iterator();
                while(iter.hasNext()){
                    EventCounter eventCounter = iter.next();
                    Summary.displaySummary(eventCounter.localCount, out);
                    out.println();
                }
            } catch (FileNotFoundException e) {
                System.err.format("Failed to write summary file %s\n", newSummaryPath);
                //e.printStackTrace();
            }
            super.visitEnd();
        }
    }

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Usage TaskSummary [rec_dir]");
        }else{
            Path toRec = Paths.get(args[0]);
            RRReader reader = new RRReader(toRec);
            TaskSummary taskSummarizer = new TaskSummary();
            reader.accept(taskSummarizer);
        }
    }
}
