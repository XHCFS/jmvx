package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class VerboseTask extends TaskVisitor {
    public VerboseTask(Optional<TaskVisitor> parent) {
        super(parent);
    }

    @Override
    public void visit(Object o) {
        System.out.println(o.getClass().getName());
        super.visit(o);
    }

    @Override
    public void visitByteArray(byte[] arr) {
        System.out.format("%d raw bytes\n", arr.length);
        super.visitByteArray(arr);
    }

    @Override
    public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
        System.out.format("%s (optimized)\n", tag.name());
        super.visitOptimized(tag, size, content);
    }

    public static void main(String args[]){
        if(args.length != 1){
            System.err.println("Please supply the file to display");
        }else{
            Path toTask = Paths.get(args[0]);
            Path toRec = toTask.getParent();
            RRReader reader = new RRReader(toRec);
            SingleTask visitor = new SingleTask(toTask, new VerboseTask(Optional.empty()));
            reader.accept(visitor);
        }
    }
}
