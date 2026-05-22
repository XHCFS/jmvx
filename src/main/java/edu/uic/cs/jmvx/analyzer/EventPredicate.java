package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.util.Optional;

public abstract class EventPredicate extends TaskVisitor{

    private boolean result = false;

    public EventPredicate(Optional<TaskVisitor> parent) {
        super(parent);
    }

    public abstract boolean predicate(Object o);
    public abstract boolean predicate(OptimizedEvent tag, int size, DataInput content);

    @Override
    public void visit(Object o) {
        result = predicate(o);
        super.visit(o);
    }

    @Override
    public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
        result = predicate(tag, size, content);
        super.visitOptimized(tag, size, content);
    }

    public boolean getResult() {
        return result;
    }
}
