package edu.uic.cs.jmvx.analyzer;

import java.util.Optional;

public class LoaderVisitor {
    protected final Optional<LoaderVisitor> parent;

    public LoaderVisitor(Optional<LoaderVisitor> parent) {
        this.parent = parent;
    }

    public Optional<TaskVisitor> visitLoadTask(String loaderClassName, String className) {
        return parent.flatMap((v) -> v.visitLoadTask(loaderClassName, className));
    }

    public void visitEnd() {
        parent.ifPresent((v) -> v.visitEnd());
    }
}
