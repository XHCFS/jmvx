package edu.uic.cs.jmvx.analyzer.util;

/*
Dealing with exceptions for IO all over the place is annoying
This wraps ObjectOutputStream and provides wrappers for common functions JMVX needs

This class catches errors from commonly used methods
Once an error occurs, we abandon the object stream.
Any additional calls are skipped.
 */

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.Callable;

public class RecordingOutputStream {

    private Throwable lastException = null;
    private boolean cont = true;
    private ObjectOutputStream out;

    public RecordingOutputStream(OutputStream base) {
        attempt(() -> {
            out = new ObjectOutputStream(base);
            return true;
        }, "Failed to create RecordingOutputStream");
    }

    public void writeObject(Object o){
        attempt(() -> {
            out.writeObject(o); //returns void
            return true; //need to return something
        }, "Failed RecordingOutputStream.writeObject");
    }

    public void write(byte[] bytes){
        attempt(() -> {
            out.write(bytes);
            return true;
        }, "Failed RecordingOutputStream.writeBytes");
    }

    public void writeByte(int v){
        attempt(() -> {
            out.write(v);
            return true;
        }, "Failed RecordingOutputStream.writeByte)");
    }

    public void writeInt(int val){
        attempt(() -> {
            out.writeInt(val);
            return true;
        }, "Failed RecordingOutputStream.writeInt");
    }

    //Use Callable because it can throw an exception
    private<V> Optional<V> attempt(Callable<V> f, String errorMessage){
        //failed a prior operation, so we stop
        if(!cont)
            return Optional.empty();

        try {
            return Optional.of(f.call());
        } catch (Exception e) {
            System.err.println(errorMessage);
            lastException = e;
            cont = false;
            return Optional.empty();
        }
    }

    public void reset(){
        attempt(() -> {
            out.reset();
            return true;
        }, "Failed RecordingOutputStream.reset");
    }

    public void flush(){
        attempt(() -> {
            out.flush();
            return true;
        }, "Failed RecordingOutputStream.flush");
    }

    public void close(){
        try {
            out.close();
        }catch (IOException _) {}
    }
}
