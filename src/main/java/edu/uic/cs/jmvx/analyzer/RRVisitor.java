package edu.uic.cs.jmvx.analyzer;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class RRVisitor {

    protected final Optional<RRVisitor> parent;

    public RRVisitor() {
        parent = Optional.empty();
    }

    public RRVisitor(Optional<RRVisitor> parent) {
        this.parent = parent;
    }

    public Optional<TaskVisitor> visitTask(Path toTask){
        return parent.flatMap((p) -> p.visitTask(toTask));
    }
    public Optional<LoaderVisitor> visitLoader() {
        return parent.flatMap((p) -> p.visitLoader());
    }
    public Optional<ClinitVisitor> visitClinits() {
        return parent.flatMap((p) -> p.visitClinits());
    }
    public Optional<Callable> visitOther(Path path) { return parent.flatMap((p) -> p.visitOther(path)); }
}
