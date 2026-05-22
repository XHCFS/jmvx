package edu.uic.cs.jmvx.analyzer;


import edu.uic.cs.jmvx.analyzer.util.RecordingOutputStream;

import java.io.*;
import java.nio.file.*;
import java.util.Optional;
import java.util.concurrent.Callable;

/*
This is missing a way to process some files right now. Eg jar path rename map.
 */
public class RRWriter extends RRVisitor {

    //directory to write to
    protected final Path recordingDir;

    //no parent because this should be the last in the chain
    public RRWriter(Path recordingDir) {
        this.recordingDir = recordingDir;
        recordingDir.toFile().mkdirs(); //ensure the directory exists
    }

    public Path adjustPath(Path original){
        return recordingDir.resolve(original.subpath(1, original.getNameCount()));
    }

    /*
    Attempt to make a file output stream.
    If that fails, makes a byte array stream instead
    This failure case could be a bad move. Maybe we should just throw a runtime error
     */
    public static OutputStream attemptFileOutputStream(File f){
        try{
            return new FileOutputStream(f);
        }catch (FileNotFoundException e){
            System.err.format("Failed to create %s, defaulting to in memory stream\n", f);
            return new ByteArrayOutputStream();
        }
    }

    public static ObjectOutputStream attemptObjectOutputStream(OutputStream base){
        try{
            return new ObjectOutputStream(base);
        }catch (IOException e){
            System.err.println("Failed to create object output stream");
            throw new RuntimeException();
        }
    }

    /*
    Makes an object output stream, either to a file or byte array stream
     */
    private RecordingOutputStream makeRecordingOutputStream(Path toTask){
        OutputStream base = attemptFileOutputStream(adjustPath(toTask).toFile());
        return new RecordingOutputStream(new BufferedOutputStream(base));
    }

    @Override
    public Optional<TaskVisitor> visitTask(Path toTask) {
        //orElse means we are writing to an in memory stream, so we return the same path as the "new" path
        //TODO: FIX BUG HERE, we are dupping path munging.
        Path toNewTask = adjustPath(toTask); //recordingDir.map((p) -> p.resolve(toTask.getFileName())).orElse(toTask);
        toNewTask.getParent().toFile().mkdirs();
        RecordingOutputStream out = makeRecordingOutputStream(toNewTask);
        return Optional.of(new TaskWriter(out));
    }

    @Override
    public Optional<LoaderVisitor> visitLoader() {
        Path loaderLog = Paths.get(RRReader.LOADER_DAT);
        Path toNewLoaderLog = recordingDir.resolve(loaderLog);
        RecordingOutputStream out = makeRecordingOutputStream(toNewLoaderLog);
        return Optional.of(new LoaderWriter(out));
    }

    @Override
    public Optional<ClinitVisitor> visitClinits() {
        Path clinitLog = Paths.get(RRReader.CLINITS_DAT);
        Path toNewClinitLog = recordingDir.resolve(clinitLog);
        RecordingOutputStream out = makeRecordingOutputStream(toNewClinitLog);
        return Optional.of(new ClinitWriter(out));
    }

    @Override
    public Optional<Callable> visitOther(Path path) {
        return Optional.of(() -> {
            Path destination = adjustPath(path);
            try {
                Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
            }catch (IOException e){
                String errMsg = String.format("Failed to copy %s to %s\n", path, destination);
                System.err.print(errMsg);
                throw new Exception(errMsg, e);
            }
            return Void.TYPE;
        });
    }
}
