package edu.uic.cs.jmvx.runtime.util;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;

import java.io.*;

public enum OptimizedEvent {
    READ,
    RANDOM_ACCESS_READ,
    ELIDED_READ, //read op skipped by Hybrid MVX/RR
    ELIDED_RANDOM_ACCESS_READ,
    WRITE,
    RANDOM_ACCESS_WRITE,
    WRITE_NO_VALIDATION,
    ELIDED_WRITE, //write op skipped by Hybrid MVX/RR
    ELIDED_RANDOM_ACCESS_WRITE, //write op skipped by Hybrid MVX/RR
    AVAILABLE,
    ELIDED_AVAILABLE, //skipped by Hybrid MVX/RR
    MONITOR_ENTER,
    FAST_MONITOR_ENTER, //skips sync
    /* Note: order of WAIT, FAST_WAIT, SINGLE_THREADED in enum matters
    Replayer uses the ordinal value to check for a valid input from the log*/
    WAIT,
    FAST_WAIT, //skips sync
    SINGLE_THREADED, //wait and monitor enter avoid the vector clock
    SEEK,
    CURRENT_TIME,
    EXCEPTION, //serialized exception comes next. Avoids need for size based flags
    LENGTH; //sentient to know the number of tags, useful for error checking on decode.

    /* What if we taint the tag rather than having multiple tags?
     * eg we have 1 READ tag. But we set the highest bit to represent ELIDED and the second highest for RANDOM_ACCESS
     * Could we remove some checks then? Then we just need to check if the main tag is READ and move on
     * The analyzer gets the extra info though.
     */

    /*
    Uncertain if OptimizedEvent.values() makes new memory on every call
    Let's cache the values here as they don't change
     */
    private static final OptimizedEvent[] values = OptimizedEvent.values();
    /*
    Also avoid extra method calls to ordinal by caching this
     */
    private static final int LEN = OptimizedEvent.LENGTH.ordinal();

    /* Size of the tags (in bytes) written to a file/buffer */
    public static final int TAG_SIZE = Byte.BYTES;

    public static void writeTag(OptimizedEvent tag, DataOutput out) throws IOException {
        out.writeByte(tag.ordinal());
    }

    public static OptimizedEvent fromByte(byte b) {
        if(b < 0 || b > LEN)
            throw new IndexOutOfBoundsException("Bad tag for OptimizedEvent");
        return values[b];
    }

    public static OptimizedEvent readTag(DataInput in) throws IOException{
        byte ord = in.readByte();
        return fromByte(ord);
    }

    public static OptimizedEvent getTagFromBuffer(CircularBuffer buffer) {
        byte[] data = buffer.readData(true);
        return OptimizedEvent.fromByte(data[0]);
    }

    public static void sendTagOverBuffer(CircularBuffer buffer, OptimizedEvent tag) {
        buffer.writeData(new byte[]{(byte) tag.ordinal()}, true);
    }
}
