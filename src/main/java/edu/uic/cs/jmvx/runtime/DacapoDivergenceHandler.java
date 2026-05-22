package edu.uic.cs.jmvx.runtime;

import edu.uic.cs.jmvx.runtime.strategy.Follower;
import edu.uic.cs.jmvx.runtime.strategy.HandlerStatus;
import edu.uic.cs.jmvx.runtime.strategy.Metadata;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.List;

public class DacapoDivergenceHandler implements DivergenceHandler {

    public static final String DIVERGENCE_HANDLED_MSG = "and divergence handled ";
    private static Logger log = Logger.getLogger(DivergenceHandler.class.getName());
    private int readCount = 0;

    public HandlerStatus<Integer> handleReadDivergence(List<StackTraceElement> stack, Metadata metadata) {
        log.warn("external read divergence handler called.");
        if (readCount == 0) {
            try {
                // on the first read, call the actual read() method using the inputstream that was passed in
                int r = JMVXRuntime.read((InputStream) metadata.is, metadata.buffer, 0, 10);
                readCount++;
                return new HandlerStatus<>(DivergenceStatus.STATUS.OK, new Integer(r));
            } catch (Exception e) {
                System.out.println(e);
                throw new RuntimeException(e);
            }
        } else {
            // on the second read, don't do anything, just log a message
            log.debug("Divergence tolerated!!!");
        }
        return new HandlerStatus<>(DivergenceStatus.STATUS.OK, new Integer(0));
    }

    public HandlerStatus<Integer> handleWriteDivergence(List<StackTraceElement> stack, Metadata metadata) {
        log.warn("external write divergence handler called.");

        if (stack.size() < 10) return DivergenceHandler.super.handleWriteDivergence(stack, metadata);

        StackTraceElement stackFrame = stack.get(17);

        // When there is a write divergence at the end of batik, append an extra string to the follower's
        // version of the message to show that this method ran successfully
        // This has to be done at the byte level
        if (stackFrame.getClassName() == "org.dacapo.harness.Callback"  && stackFrame.getMethodName() == "complete") {
            addDivergenceMessage(metadata);

            return new HandlerStatus<>(DivergenceStatus.STATUS.OK, new Integer(0)); //CoreDumpCallback.memDump
        }else if(stackFrame.getClassName().equals("CoreDumpCallback") && stackFrame.getMethodName().equals("memDump")){
            //for MX heap info
            return new HandlerStatus<>(DivergenceStatus.STATUS.OK, 0);
        }

        return DivergenceHandler.super.handleWriteDivergence(stack, metadata);
    }

    /**
     * Add a divergence handled message to the metadata.
     * This method modifies the metadata object, sometimes destructively
     * eg non-printed bytes before the offset index are dropped
     * if the buffer cannot accommodate the divergence message.
     * @param metadata Metadata about a write divergence
     */
    public static void addDivergenceMessage(Metadata metadata) {
        byte[] bytes = DIVERGENCE_HANDLED_MSG.getBytes();
        int newWriteSize = metadata.length + bytes.length;
        int bufferLeft = metadata.buffer.length - metadata.offset - newWriteSize;
        if(bufferLeft >= 0) {
            //room in place
            int endOfBuffer = metadata.offset + metadata.length;
            for (int i = 0; i < bytes.length; i++) {
                metadata.buffer[i + endOfBuffer] = bytes[i];
            }
            metadata.length += bytes.length;
        }else{
            //not enough room in the buffer, provide a new buffer
            //NOTE: metadata is only used for printing and does not affect the underlying object
            //so we can get away with copying only the data we wish to write
            byte[] newBuffer = new byte[newWriteSize];
            //copy from metadata.buffer into newBuffer
            System.arraycopy(metadata.buffer, metadata.offset, newBuffer, 0, metadata.length);
            //copy message to newBuffer, starting at end
            System.arraycopy(bytes, 0, newBuffer, metadata.length, bytes.length);
            //adjust the args to the subsequent write
            metadata.buffer = newBuffer;
            metadata.offset = 0;
            metadata.length = newWriteSize;
        }
    }
}
