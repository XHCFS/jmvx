package edu.uic.cs.jmvx;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.ZipFile;

import edu.uic.cs.jmvx.runtime.JMVXInputStream;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import sun.nio.ch.ChannelInputStream;

public class Utils {
    // from: https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte[] getChecksum(byte[] b, CRC32 crc32) {
        crc32.reset();
        crc32.update(b);
        return longToBytes(crc32.getValue());
    }

    public static byte[] getChecksum(byte[] b, int off, int len, CRC32 crc32) {
        crc32.reset();
        crc32.update(b, off, len);
        return longToBytes(crc32.getValue());
    }

    public static boolean isInputStreamFromDisk(JMVXInputStream is) {
        /* Explicit check for ChannelInputStream is a hack for dacapo
         * which uses it to wrap a FileChannelImpl
         * However, ChannelInputStream relies on InputStream.read(byte[]), so just adding to a deny list
         * causes us to leave a path for ChannelInputStream events to be logged
         * If we detect it here, we can avoid the issue
         */
        if (is instanceof java.io.FileInputStream ||
                is instanceof ChannelInputStream  ||
                is.getClass().equals(JMVXRuntime.zipFileInputStreamClass)) {
            return true;
        }
        return false;
    }

}