package edu.uic.cs.jmvx.runtime.util.recording;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;

public class ReadRecorderUtility extends ExceptionRecorderUtility{

    protected final boolean useBuffer;
    private static final byte[] NO_BYTES = {};

    public ReadRecorderUtility(boolean avoidObjectSerialization, boolean circularBufferEnabled, boolean flushAfterRecording) {
        super(avoidObjectSerialization, flushAfterRecording);
        this.useBuffer = avoidObjectSerialization && circularBufferEnabled; //we can only write raw bytes to the buffer
    }

    /**
     * Catch all for recording/logging logic
     * Static method that can be called for any strategy that will handle recording in mvx or rr specific way
     * @param out ObjectOutputStream to write to (if needed)
     *            this stream is used unless avoidObjectSerialization and circularBufferEnabled are both true
     * @param buffer CircularBuffer to write to
     * @param tag Tag of the optimized event (eg READ or RANDOM_ACCESS_READ)
     * @param bytes buffer of bytes AFTER the read
     * @param off offset used in read
     * @param len length used in read
     * @param bytesRead number of bytes read. Normally this is the return value of read
     */

    public void recordRead(ObjectOutputStream out, CircularBuffer buffer,
                                  OptimizedEvent tag, byte[] bytes, int off, int len, int bytesRead){
        if(useBuffer){
            recordReadOverBuffer(buffer, tag, bytes, off, len, bytesRead);
        }else if(avoidObjectSerialization){
            recordReadAsTaggedUnion(out, tag, bytes, off, len, bytesRead);
        }else{
            recordReadAsObject(out, tag, bytes, off, len, bytesRead);
        }
    }

    public void recordReadByte(ObjectOutputStream out, CircularBuffer buffer, OptimizedEvent tag, int b){
        /*semantics for this call differ!
         *return is the byte actually read it. Rather than special case the logging, we're special casing our call
         * Logging works as usual, but the ReadReplayerUtility will decode the return value specially for us */
        byte[] bytes = intToBuffer(b);
        recordRead(out, buffer, tag, bytes, 0, Integer.BYTES, Integer.BYTES);
    }

    public void recordReadAsObject(ObjectOutputStream out, OptimizedEvent tag, byte[] bytes, int off, int len, int ret){
        try {
            out.writeObject(new BufferEvent(tag, bytes, off, len, ret));
            out.reset();

            if(flushAfterRecording) //no need to flush if we wrote over the circular buffer directly
                out.flush();
        }catch (IOException e){
            throw new RuntimeException("Failed to write read event to output stream", e);
        }
    }

    public void recordReadAsTaggedUnion(ObjectOutputStream out, OptimizedEvent tag, byte[] bytes, int off, int len, int ret){
        try {
            boolean hasPayload = ret > 0;
            OptimizedEvent.writeTag(tag, out);
            out.writeInt(
                    (Integer.BYTES * 3) //off, len, ret
                            + (hasPayload ? ret: 0) //bytes
            );
            out.writeInt(off);
            out.writeInt(len);
            out.writeInt(ret);
            if(ret > 0)
                out.write(bytes, off, ret);

            if(flushAfterRecording) //no need to flush if we wrote over the circular buffer directly
                out.flush();
        }catch (IOException e){
            throw new RuntimeException("Failed to write to log file", e);
        }
    }

    public void recordReadOverBuffer(CircularBuffer buffer, OptimizedEvent tag, byte[] bytes, int off, int len, int ret){
        sendTagOverBuffer(buffer, tag);
        byte[] toSend = bytes;
        if(ret < len){
            //send subset of bytes or none if the read failed
            toSend = ret > 0 ? Arrays.copyOfRange(bytes, off, off + ret) : NO_BYTES;
        }
        buffer.writeWriteOpData(toSend, off, ret, true);
        //buffer has no flush operation
    }

    public static void sendTagOverBuffer(CircularBuffer buffer, OptimizedEvent tag) {
        buffer.writeData(new byte[]{(byte) tag.ordinal()}, true);
        //buffer has no flush operation
    }

    byte[] reusableIntBuffer = new byte[Integer.BYTES];

    public byte[] intToBuffer(int i){
        //byte data[] = new byte[Integer.BYTES];
        JMVXRuntime.unsafe.putInt(reusableIntBuffer, Unsafe.ARRAY_BYTE_BASE_OFFSET, i);
        return reusableIntBuffer;
    }
}
