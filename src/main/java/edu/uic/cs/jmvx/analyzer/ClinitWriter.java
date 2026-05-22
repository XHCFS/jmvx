package edu.uic.cs.jmvx.analyzer;

import edu.uic.cs.jmvx.analyzer.util.RecordingOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Optional;

/*
Very similar to LoaderWriter
In fact they both suffer from the "extra byte at end of Copier analysis"
See the note in LoaderWriter
 */
public class ClinitWriter extends ClinitVisitor {

    protected final RecordingOutputStream clinitsOut;

    public ClinitWriter(RecordingOutputStream out) {
        super(Optional.empty());
        clinitsOut = out;
    }

    @Override
    public Optional<TaskVisitor> visitClinitTask(String className) {
        clinitsOut.writeObject(className);
        return Optional.of(new InnerTaskWriter(clinitsOut, new ByteArrayOutputStream()));
    }

    @Override
    public void visitEnd() {
        clinitsOut.close();
        super.visitEnd();
    }
}
