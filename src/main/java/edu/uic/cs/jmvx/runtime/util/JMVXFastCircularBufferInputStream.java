package edu.uic.cs.jmvx.runtime.util;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;

import java.io.IOException;
import java.io.InputStream;

/**
 * A byte buffer which stores a single entry written to the ring buffer at a time
 * "Faster" than the old method because the byte buffer is kept around and reused
 * thereby reducing time and resources wasted on allocation
 *
 * To use the circular buffer directly requires additional modifications.
 * It was not designed with partial entries in mind
 */
public class JMVXFastCircularBufferInputStream extends InputStream {

    private static final int DEFAULT_BUFFER_INIT_SIZE = 8192; //taken from BufferedInputStream
    private static final int GROWTH_FACTOR = 1; //increases the buffer size by this multiple of min capacity.
    protected volatile byte[] buf = new byte[DEFAULT_BUFFER_INIT_SIZE];
    protected int pos = 0; //how many bytes have been read from the populated portion of the buffer
    protected int count = 0; //how many bytes from the circular buffer are in the byte buffer
    protected final CircularBuffer byteSource;

    public JMVXFastCircularBufferInputStream(CircularBuffer byteSource) {
        this.byteSource = byteSource;
    }

    private boolean ensureEntry(){
        if(pos >= count){
            boolean resized = false;
            //reset
            pos = 0;
            while(true) {
                int read = byteSource.readData(buf, 0, buf.length);
                if (read < 0) {
                    //need to resize
                    buf = new byte[-read * GROWTH_FACTOR];
                    resized = true;
                } else {
                    //success!
                    count = read;
                    return resized;
                }
                //TODO?, check if more entries are present, and try to grab them if room is in the buffer
                //goal should be to fill up as much of the buffer as we can once we have to ensure the entry
            }
        }else{
            return false; //unread data, don't need to repopulate yet
        }
    }

    @Override
    public int read() throws IOException {
        ensureEntry();
        int readable = available();
        if(readable >= 1)
            return buf[pos++];
        else
            throw new IOException("Cannot read");
    }

    @Override
    public int read(byte[] b) throws IOException{
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException{
        ensureEntry();
        int readable = available();
        if(readable > 0) {
            int read = Math.min(readable, len);
            System.arraycopy(buf, pos, b, off, read);
            pos += read;
            return read;
        } else if(readable == 0)
            return 0;
        else {
            throw new IOException("Cannot read");
        }
    }

    @Override
    public int available() {
        return count > pos ? count - pos : 0;
    }


}
