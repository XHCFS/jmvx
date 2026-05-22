package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.analyzer.util.RecordingOutputStream;

import java.io.ByteArrayOutputStream;
import java.util.Optional;

/*
Like TaskWriter, assume that this is the last in the chain

NOTE: analysis can result in the files having one byte extra at the end
JMVX may not gracefully close the loader output stream (leaving it to the JVM on exit)
This causes a terminating byte to be missing in the original recording, which is added after analysis.
 */
public class LoaderWriter extends LoaderVisitor{

    protected final RecordingOutputStream loaderOut;

    public LoaderWriter(RecordingOutputStream out) {
        super(Optional.empty());
        this.loaderOut = out;
    }

    @Override
    public Optional<TaskVisitor> visitLoadTask(String loaderClassName, String className) {
        //visit end of LoadTaskWriter handles writing the bytes of load operations
        loaderOut.writeObject(loaderClassName);
        loaderOut.writeObject(className);
        return Optional.of(new InnerTaskWriter(loaderOut, new ByteArrayOutputStream()));
    }

    @Override
    public void visitEnd() {
        loaderOut.close();
        super.visitEnd();
    }
}
