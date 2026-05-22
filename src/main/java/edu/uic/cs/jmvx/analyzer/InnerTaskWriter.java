package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.analyzer.util.RecordingOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Optional;

/*
TaskWriter which writes to its own byte array
and then writes that byte array (as an object)
to another stream

Used by LoaderWriter and ClinitWriter
 */
class InnerTaskWriter extends TaskWriter {
    //assumption is writeTo is an ObjectStream around bytesOut
    protected final ByteArrayOutputStream bytesOut;
    protected final RecordingOutputStream writeDestination;

    public InnerTaskWriter(RecordingOutputStream writeDestination, ByteArrayOutputStream bytesOut) {
        /*
        bytesOut is a hack. We need a reference to the ByteArrayOutputStream
        underlying the stream in the parent class.
        So we make it elsewhere and pass it in via the constructor.

        this.out in LoadTaskWriter is going to build up a byte array
        We will extract the array from bytesOut
        and later write it to loaderOut in the containing class.
         */
        super(new RecordingOutputStream(new BufferedOutputStream(bytesOut)));
        this.bytesOut = bytesOut;
        this.writeDestination = writeDestination;
    }

    @Override
    public void visitHeader(Optional<Integer> clockIndex) {
        out.writeObject(null);
        //no call to reset
    }

    @Override
    public void visitEnd() {
        super.visitEnd(); //closes the stream the task is being written to
        //write this to the LoaderWriter's output
        writeDestination.writeObject(bytesOut.toByteArray());
        writeDestination.reset();
    }
}
