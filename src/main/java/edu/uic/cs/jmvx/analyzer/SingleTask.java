package edu.uic.cs.jmvx.analyzer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/*
Processes a single task based on the path to the dat file
 */
public class SingleTask extends RRVisitor {

    private final Path toTask;
    private final TaskVisitor visitor;

    public SingleTask(Path toTask, TaskVisitor visitor) {
        this.toTask = toTask;
        this.visitor = visitor;
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        if(toTask.equals(this.toTask))
            return Optional.of(visitor);
        else return Optional.empty();
    }

    /*
    We end up losing info with this implementation for visitLoader and visitClinits
    You need to override those methods to get all info verbosely, not just events.
     */
    @Override
    public Optional<LoaderVisitor> visitLoader() {
        if(toTask.endsWith(Paths.get(RRReader.LOADER_DAT)))
            return Optional.of(new LoaderVisitor(super.visitLoader()){
                @Override
                public Optional<TaskVisitor> visitLoadTask(String loaderClassName, String className) {
                    return Optional.of(visitor);
                }
            });
        return Optional.empty();
    }

    @Override
    public Optional<ClinitVisitor> visitClinits() {
        if(toTask.endsWith(Paths.get(RRReader.CLINITS_DAT)))
            return Optional.of(new ClinitVisitor(super.visitClinits()){
                @Override
                public Optional<TaskVisitor> visitClinitTask(String className) {
                    return Optional.of(visitor);
                }
            });
        return Optional.empty();
    }
}
