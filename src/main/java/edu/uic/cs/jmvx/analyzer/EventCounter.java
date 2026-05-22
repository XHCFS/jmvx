package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.util.HashMap;
import java.util.Optional;

public class EventCounter extends TaskVisitor{
    //local hash map to alleviate need for a lock
    //while processing
    HashMap<String, Long> localCount = new HashMap<>();

    public EventCounter(Optional<TaskVisitor> parent) {
        super(parent);
    }

    private void countEvent(String eventName) {
        Long val = localCount.getOrDefault(eventName, 0L) + 1;
        localCount.put(eventName, val);
    }

    @Override
    public void visit(Object o) {
        String className = o.getClass().getName();
        countEvent(className);
        super.visit(o);
    }

    @Override
    public void visitByteArray(byte[] arr) {
        countEvent("rawBytes");
        super.visitByteArray(arr);
    }

    @Override
    public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
        countEvent(tag.name());
        super.visitOptimized(tag, size, content);
    }
}
