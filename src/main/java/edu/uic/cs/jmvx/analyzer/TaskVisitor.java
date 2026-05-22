package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.DataInput;
import java.util.Optional;

//is attempting to mimic asm overkill?
public class TaskVisitor {

    protected final Optional<TaskVisitor> parent;

    public TaskVisitor(Optional<TaskVisitor> parent) {
        this.parent = parent;
    }

    public void visit(Object o){
        parent.ifPresent((v) -> v.visit(o));
    }

    //data is embedded in the path of the recording file, so we pass it here
    public void visitPoolInfo(int pooldId, int stamp){ parent.ifPresent((v) -> v.visitPoolInfo(pooldId, stamp)); }

    public void visitHeader(Optional<Integer> clockIndex){
        parent.ifPresent((v) -> v.visitHeader(clockIndex));
    }

    //read or write
    /*
    Catch all right now. We cannot tell apart optimized log entries from one another
    They all just look like byte[]. When we add tagged unions, this will change (or more methods will be added)
     */
    @Deprecated
    public void visitByteArray(byte[] arr){
        parent.ifPresent((v) -> v.visitByteArray(arr));
    }

    /*
    Visit a section of raw data, represented an optimized write
    Eg the event content is not a serialized object one or more pieces of primitive data
     */
    public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
        parent.ifPresent((v) -> v.visitOptimized(tag, size, content));
    }

    public void visitEnd(){
        parent.ifPresent((v) -> v.visitEnd());
    }
}
