package edu.uic.cs.jmvx.analyzer;

import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Summary extends RRVisitor{
    public final HashMap<String, Long> count = new HashMap<>();

    private class AccumulatingEventCounter extends EventCounter{
        public AccumulatingEventCounter(Optional<TaskVisitor> parent) {
            super(parent);
        }

        @Override
        public void visitEnd() {
            //update global count with values from localCount
            synchronized (count){
                localCount.forEach((className, c) ->
                        count.put(className, count.getOrDefault(className, 0L) + c));
            }
            super.visitEnd();
        }
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        //System.out.println("Visiting: " + toTask.getFileName().toString());
        return Optional.of(new AccumulatingEventCounter(super.visitTask(toTask)));
    }

    @Override
    public Optional<LoaderVisitor> visitLoader() {
        //System.out.println("Visiting loader");
        return Optional.of(new LoaderVisitor(super.visitLoader()){
            @Override
            public Optional<TaskVisitor> visitLoadTask(String loaderClassName, String className) {
                return Optional.of(new AccumulatingEventCounter(super.visitLoadTask(loaderClassName, className)));
            }
        });
    }

    @Override
    public Optional<ClinitVisitor> visitClinits() {
        //System.out.println("Visiting clinits");
        return Optional.of(new ClinitVisitor(super.visitClinits()){
            @Override
            public Optional<TaskVisitor> visitClinitTask(String className) {
                return Optional.of(new AccumulatingEventCounter(super.visitClinitTask(className)));
            }
        });
    }

    public static void displaySummary(Map<String, Long> count, PrintStream out) {
        ArrayList<Map.Entry<String, Long>> events = new ArrayList<>(count.entrySet());
        Collections.sort(events, Comparator.comparing(Map.Entry::getValue));
        double total = events.stream().map(Map.Entry::getValue).reduce(0L, Long::sum);
        events.stream().forEach(e -> out.println("" + e + "\t\t" + ((double)e.getValue())/total));
    }

    public void displaySummary(PrintStream out) {
        displaySummary(count, out);
    }

    public void displaySummary() {
        displaySummary(count, System.out);
    }

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Please supply the directory to a replay");
        }else{
            Path toRec = Paths.get(args[0]);
            RRReader reader = new RRReader(toRec);
            Summary summarizer = new Summary();
            reader.accept(summarizer);

            summarizer.displaySummary();
        }
    }
}
