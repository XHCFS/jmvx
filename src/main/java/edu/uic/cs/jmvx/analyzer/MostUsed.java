package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.strategy.Leader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MostUsed extends RRVisitor{

    ConcurrentHashMap<Path, Long> summaries = new ConcurrentHashMap<>();
    private final Class<?> lookFor;

    public MostUsed(Class<?> lookFor) {
        this.lookFor = lookFor;
    }

    private class AccumulatingEventCounter extends EventCounter {

        private final Path toTask;

        public AccumulatingEventCounter(Optional<TaskVisitor> parent, Path toTask) {
            super(parent);
            this.toTask = toTask;
        }

        @Override
        public void visitEnd() {
            summaries.put(toTask, localCount.getOrDefault(lookFor.getName(), 0L));
            super.visitEnd();
        }
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        return Optional.of(new AccumulatingEventCounter(super.visitTask(toTask), toTask));
    }

    public void displayOrder(){
        ArrayList<Map.Entry<Path, Long>> events = new ArrayList<>(summaries.entrySet());
        Collections.sort(events, Comparator.comparingLong(Map.Entry<Path, Long>::getValue));
        events.stream().forEach(e -> System.out.format("%s\t%d\n", e.getKey(), e.getValue()));
    }

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Usage MostUsed [rec_dir]");
        }else{
            Path toRec = Paths.get(args[0]);
            RRReader reader = new RRReader(toRec);
            MostUsed visitor = new MostUsed(Leader.MonitorEnter.class);
            reader.accept(visitor);

            visitor.displayOrder();
        }
    }
}
