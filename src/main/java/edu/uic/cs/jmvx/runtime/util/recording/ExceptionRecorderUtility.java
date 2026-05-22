package edu.uic.cs.jmvx.runtime.util.recording;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class ExceptionRecorderUtility {

    protected final boolean avoidObjectSerialization;
    protected final boolean flushAfterRecording;

    public ExceptionRecorderUtility(boolean avoidObjectSerialization, boolean flushAfterRecording) {
        this.avoidObjectSerialization = avoidObjectSerialization;
        this.flushAfterRecording = flushAfterRecording;
    }

    /** Writes a sigil OptimizedEvent.EXCEPTION tag to indicate an exception is coming
     * if avoidObjectSerialization is on. Otherwise, it just logs an exception
     * This method should only be used by events that have an optimized representation
     * eg, those which have special logic for avoidObjectSerialization */
    public void logException(ObjectOutputStream out, Exception e) {
        try {
            if (avoidObjectSerialization) {
                OptimizedEvent.writeTag(OptimizedEvent.EXCEPTION, out);
            }
            out.writeObject(e);
            if(flushAfterRecording)
                out.flush();
        }catch (IOException ex){
            throw new RuntimeException("Failed to write to output stream", ex);
        }
    }
}
