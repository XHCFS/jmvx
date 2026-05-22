package edu.uic.cs.jmvx.runtime.util;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * ByteArrayOutputStream that dumps its buffer on the circular buffer
 * This is "faster" than the old method because the buffer is reused.
 * Reduce the amount of memory allocation that occurs
 *
 * To use the circular buffer directly requires additional modifications.
 * It was not designed with partial entries in mind.
 * This class stages writes until a flush, and creates one large single entry in the buffer.
 */
public class JMVXFastCircularBufferOutputStream extends ByteArrayOutputStream {

    private final static int DEFAULT_BUFFER_INIT_SIZE = 8192; //taken from BufferedInputStream
    protected final CircularBuffer byteSink;

    public JMVXFastCircularBufferOutputStream(CircularBuffer byteSink) {
        super(DEFAULT_BUFFER_INIT_SIZE);
        this.byteSink = byteSink;
    }

    @Override
    public void flush() throws IOException {
        //avoids a copy going to an array
        byteSink.writeData(this.buf, 0, this.count);
        reset();
    }

}
