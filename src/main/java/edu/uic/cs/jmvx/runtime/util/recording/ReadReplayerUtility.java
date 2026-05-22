package edu.uic.cs.jmvx.runtime.util.recording;

import edu.uic.cs.jmvx.circularbuffer.CircularBuffer;
import edu.uic.cs.jmvx.runtime.DivergenceError;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Decodes a read into fields of this object and inspects them
 * Each Follower/Replayer should have a unique instance of this object.
 * This way, a single thread can divert its read event decoding to here
 */
public class ReadReplayerUtility {
    protected final boolean useBuffer;
    protected final boolean avoidObjectSerialization;

    private static final byte[] NO_BYTES = {};

    //lets us know how many times this has been used
    int generation = 0;

    //private byte[] data = NO_BYTES; //cached buffer to use with the circular buffer

    public ReadReplayerUtility(boolean avoidObjectSerialization, boolean circularBufferEnabled) {
        this.avoidObjectSerialization = avoidObjectSerialization;
        this.useBuffer = avoidObjectSerialization && circularBufferEnabled;
    }

    /**
     * Replays a read operation. Can read data in from a circular buffer or object stream depending on flags (and
     * presence of an exception).
     * @param in ObjectInputStream to read from. Should always be supplied as it is used to read exceptions
     * @param buffer CircularBuffer. Data is only read from it if avoidObjectSerialization
     *               and circularBufferedEnabled are true.
     * @param isRandomAccess true if the operation belongs to a RandomAccessFile (false for InputStream)
     * @param bytes buffer for the read call
     * @param off offset of the read call
     * @param len length of the read call
     * @return number of bytes read, or -1 if EOF
     * @throws IOException when an IOException was logged
     * @throws DivergenceError if a different event was read
     * @throws RuntimeException if an IOException happens within JMVX
     */
    public int replayRead(ObjectInputStream in, CircularBuffer buffer, boolean isRandomAccess,
                          byte[] bytes, int off, int len) throws IOException {
        try {
            if (useBuffer) {
                return decodeFromBuffer(in, buffer, isRandomAccess, bytes, off, len);
            } else if (avoidObjectSerialization) {
                return decodeFromTaggedUnion(in, isRandomAccess, bytes, off, len);
            } else {
                return decodeFromObject(in, isRandomAccess, bytes, off, len);
            }
        }catch (ThrowsIOException e){
            throw e.getException();
        }
    }

    private byte[] reusableIntBuffer = new byte[Integer.BYTES];

    /**
     * Replays a read of a single byte
     * Implemented separately because the return value has different semantics, eg the byte read is returned
     * rather than the number of bytes.
     * Internally, this uses replayRead to grab the data, but properly enocdes the return value for simpler
     * event handlers
     * The arguments have the same meaning as those in replayRead
     * @param in
     * @param buffer
     * @param isRandomAccess true if the operation belongs to a RandomAccessFile (false for InputStream)
     * @return
     * @throws IOException
     */
    public int replayReadByte(ObjectInputStream in, CircularBuffer buffer, boolean isRandomAccess)
            throws IOException, DivergenceError, RuntimeException {
        try {
            if (useBuffer) {
                decodeFromBuffer(in, buffer, isRandomAccess, reusableIntBuffer, 0, Integer.BYTES);
            } else if (avoidObjectSerialization) {
                decodeFromTaggedUnion(in, isRandomAccess, reusableIntBuffer, 0, Integer.BYTES);
            } else {
                decodeFromObject(in, isRandomAccess, reusableIntBuffer, 0, Integer.BYTES);
            }
            return bufferToInt(reusableIntBuffer);
        }catch (ThrowsIOException e){
            throw e.getException();
        }
    }

    /**
     * Validates that a read tag came from a read event
     * @param tag Tag to validate
     * @param isRandomAccess true if the expected event belongs to a RandomAccessFile (false for InputStream)
     * @return boolean representing if the tag matched.
     * Note: this method returns false for BOTH exceptions and divergences.
     */
    private boolean validateTag(OptimizedEvent tag, boolean isRandomAccess) {
        switch(tag){
            case READ:
            case ELIDED_READ:
                return isRandomAccess == false;
            case RANDOM_ACCESS_READ:
            case ELIDED_RANDOM_ACCESS_READ:
                return isRandomAccess == true;
            case EXCEPTION:
            default:
                return false;
        }
    }

    /**
     * Attempts to read an IOException
     * @param in ObjectInputStream to read from
     * @return the IOException read
     * @throws DivergenceError if the event read was not an IOException
     * @throws RuntimeException when read from `in` fails
     */
    private IOException readException(ObjectInputStream in) throws DivergenceError, RuntimeException {
        try {
            IOException e = (IOException) in.readObject();
            return e;
        }catch (IOException e){
            throw new RuntimeException("Failed to read from input stream", e);
        } catch (ClassCastException | ClassNotFoundException e) {
            throw new DivergenceError();
        }
    }

    /** Check if the parameters of the read diverge from what we decoded
     * A read diverges if any of the following are true:
     *  1) the offsets in the buffer differ
     *  2) We cannot fit the bytes into the buffer supplied
     * @param off
     * @param len
     * @return
     */
    private static boolean validateProperties(int expectedOff, int expectedLen, int ret, int off, int len) {
        return expectedOff == off && ret <= len;
    }

    private int decodeFromObject(ObjectInputStream in, boolean isRandomAccess,
                                     byte[] dest, int off, int len)
            throws ThrowsIOException, DivergenceError, RuntimeException {
        generation++;
        try{
            Object o = in.readObject();
            if(o instanceof BufferEvent){
                BufferEvent event = (BufferEvent) o;
                if(!validateProperties(event.off, event.len, event.ret, off, len) ||
                        !validateTag(event.tag, isRandomAccess)){
                    throw new DivergenceError(); //check for IOException elsewhere
                }else if(event.ret > 0){
                    System.arraycopy(event.b, event.off, dest, off, event.ret);
                }
                return event.ret;
            }else if(o instanceof IOException){
                throw new ThrowsIOException((IOException) o);
            }else{
                throw new DivergenceError();
            }
        } catch(ClassCastException | ClassNotFoundException e) {
            throw new DivergenceError();
        } catch (IOException e){
            throw new RuntimeException("Failed to read data", e);
        }
    }

    //almost usable by the analyzer tool. Maybe should refactor into 2 methods so I can do that
    private int decodeFromTaggedUnion(ObjectInputStream in, boolean isRandomAccess,
                                          byte[] dest, int off, int len)
            throws ThrowsIOException, DivergenceError, RuntimeException {
        generation++;
        try {
            OptimizedEvent tag = OptimizedEvent.readTag(in);

            //check for divergence
            if(validateTag(tag, isRandomAccess)) {
                int size = in.readInt();
                //potential refactor to reuse code in analyzer from here down?
                int rOff = in.readInt();
                int rLen = in.readInt();
                int ret = in.readInt();

                if (!validateProperties(rOff, rLen, ret, off, len)) {
                    throw new DivergenceError();
                }
                if (ret > 0)
                    in.readFully(dest, off, ret);
                //else okay, just pass through to return
                return ret;
            }else{
                switch (tag){
                    case EXCEPTION:
                        throw new ThrowsIOException(readException(in));
                    default:
                        throw new DivergenceError();
                }
            }
        }catch (IOException e){
            throw new RuntimeException("Failed to read from log file", e);
        }
    }


    public int decodeFromBuffer(ObjectInputStream in, CircularBuffer buffer, boolean isRandomAccess,
                                    byte[] dest, int off, int len)
            throws ThrowsIOException, DivergenceError, RuntimeException {
        generation++;
        OptimizedEvent tag = OptimizedEvent.getTagFromBuffer(buffer);

        if (validateTag(tag, isRandomAccess)) {
            int byteOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
            byte[] data = buffer.readData(true);
            long byteLen = JMVXRuntime.unsafe.getLong(data, byteOffset);
            long otherFieldsOffset = byteOffset + Long.BYTES + byteLen;
            int receivedOff = JMVXRuntime.unsafe.getInt(data, otherFieldsOffset);
            int receivedLen = JMVXRuntime.unsafe.getInt(data, otherFieldsOffset + Integer.BYTES);
            int ret = receivedLen;

            if (!validateProperties(receivedOff, receivedLen, ret, off, len))
                throw new DivergenceError();

            //populate bytes if there are any
            if (ret > 0) {
                try {
                    //copy is of the same type, we don't need unsafe. arraycopy saves us from overflow on accident
                    System.arraycopy(data, Long.BYTES, dest, off, ret);
                } catch (Throwable t) {
                    throw new RuntimeException("Error copying bytes", t);
                }
            }

            return ret;
        }else{
            switch (tag){
                case EXCEPTION:
                    throw new ThrowsIOException(readException(in));
                default:
                    throw new DivergenceError();
            }
        }
    }

    protected class ThrowsIOException extends Exception {
        OptimizedEvent tag;
        IOException e;

        @Deprecated
        public ThrowsIOException(ObjectInputStream in, OptimizedEvent tag) {
            this.tag = tag;
            if(tag.equals(OptimizedEvent.EXCEPTION)){
                readException(in);
            }else{
                throw new DivergenceError();
            }
        }

        public ThrowsIOException(IOException e) {
            this.e = e;
            this.tag = OptimizedEvent.EXCEPTION;
        }

        private void readException(ObjectInputStream in){
            try {
                e = (IOException) in.readObject();
            }catch (IOException e){
                throw new RuntimeException("Failed to read from input stream", e);
            } catch (ClassCastException | ClassNotFoundException e) {
                throw new DivergenceError();
            }
        }

        public OptimizedEvent getTag() {
            return tag;
        }

        public IOException getException() {
            return e;
        }
    }

    public static int bufferToInt(byte[] b) {
        //can use unsafe and getInt
        return ((b[0] & 0xFF)      ) +
                ((b[1] & 0xFF) <<  8) +
                ((b[2] & 0xFF) << 16) +
                ((b[3]       ) << 24);
    }
}
