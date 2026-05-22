package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.analyzer.util.RecordingOutputStream;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.*;
import java.util.Optional;

/*
Assumed to be the last Visitor in the chain,
so it does not accept a parent
 */
public class TaskWriter extends TaskVisitor {

    protected final RecordingOutputStream out;
    protected boolean wroteHeader = false;

    public TaskWriter(RecordingOutputStream writeTo) {
        super(Optional.empty());
        out = writeTo;
    }

    @Override
    public void visit(Object o) {
        out.writeObject(o);
        /*recorder calls reset after each serialized object, we mimic.*/
        out.reset();
        //super.visit(o);
    }

    @Override
    public void visitHeader(Optional<Integer> clockIndex) {
        if(clockIndex.isPresent()){
            out.writeInt(clockIndex.get());
        }else{
            out.writeObject(null);
            out.reset();
        }
        wroteHeader = true;
        //super.visitHeader(clockIndex);
    }

    @Override
    public void visitPoolInfo(int pooldId, int stamp) {
        super.visitPoolInfo(pooldId, stamp);
    }

    @Override
    public void visitByteArray(byte[] arr) {
        /*out.write(arr);
        super.visitByteArray(arr);*/
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void visitOptimized(OptimizedEvent tag, int size, DataInput content) {
        try {
            /*a little sloppy, RRReader packages the byte array into a DataInput
            Just for us to pull all the data out into a byte array
             */
            out.writeByte(tag.ordinal()); //basically same as OptimizedEvent.writeTag, but should refactor
            if(size > 0) {
                byte[] buf = new byte[size];
                content.readFully(buf);
                out.writeInt(size);
                out.write(buf);
            }
        }catch (IOException e){
            throw new Error("Failed to parse/write optimized section", e);
        }
        //super.visitOptimized(tag, size, content);
    }

    @Override
    public void visitEnd() {
        //close the stream?
        out.close();
        //super.visitEnd();
    }
}
