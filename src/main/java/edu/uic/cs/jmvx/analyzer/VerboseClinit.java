package edu.uic.cs.jmvx.analyzer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class VerboseClinit extends RRVisitor {

    private final String clazz;

    public VerboseClinit(String clazz) {
        this(Optional.empty(), clazz);
    }

    public VerboseClinit(Optional<RRVisitor> parent, String clazz) {
        super(parent);
        this.clazz = clazz;
    }

    @Override
    public Optional<ClinitVisitor> visitClinits() {
        return Optional.of(new ClinitVisitor(super.visitClinits()){
            @Override
            public Optional<TaskVisitor> visitClinitTask(String className) {
                if(className.endsWith(clazz)){
                    /* safe without a lock, clinits are processed serially because they are part of the same file
                    * However, I probably should add synchronization to printing operations because
                    * VerboseTask does not know how it will be used by other visitors*/
                    System.out.format("%s:\n", className);
                    return Optional.of(new VerboseTask(super.visitClinitTask(className)));
                }else{
                    return Optional.empty();
                }
            }
        });
    }

    public static void main(String args[]){
        if(args.length != 2){
            System.err.println("Usage: [rec dir] [class name]");
        }else{
            Path toRec = Paths.get(args[0]);
            String className = args[1];
            RRReader reader = new RRReader(toRec);
            RRVisitor visitor = new VerboseClinit(className);
            reader.accept(visitor);
        }
    }
}
