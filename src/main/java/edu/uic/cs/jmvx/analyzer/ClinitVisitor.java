package edu.uic.cs.jmvx.analyzer;

import java.util.Optional;

public class ClinitVisitor {
    protected final Optional<ClinitVisitor> parent;

    public ClinitVisitor(Optional<ClinitVisitor> parent) {
        this.parent = parent;
    }

    public Optional<TaskVisitor> visitClinitTask(String className){
        return parent.flatMap((v) -> v.visitClinitTask(className));
    }

    public void visitEnd(){
        parent.ifPresent((v) -> v.visitEnd());
    }
}
