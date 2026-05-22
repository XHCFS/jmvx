package edu.uic.cs.jmvx.runtime.util;

import edu.uic.cs.jmvx.runtime.JMVXFile;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SeparateLoadingLogs {

    public static final String recordingDir = JMVXRuntime.parameters.recordingDir;

    public static final Path clinitsPath = Paths.get(recordingDir, "clinits");
    public static final Path classesPath = Paths.get(recordingDir, "classes");

    public static Path clinitLogPath(String cls){
        /* NOTE: using String.format leads to an error
         * The Formatter class has a null pointer exception.
         * Maybe we force it to be used too early (before dependencies are properly loaded)
         * if called from here*/
        String cleaned = cls.replace('/', '.');
        return clinitsPath.resolve(cleaned + ".dat");
    }

    public static Path classesLogPath(ClassLoader loader, String cls){
        return classesPath.resolve(loader.getClass().getSimpleName()).resolve(cls + ".dat");
    }

    public static boolean makeParentDirs(Path p){
        //TODO: cache this? if the parent of p has been made before, why bother with any potential IO
        //check a concurrent hashset and passthrough if it exists
        JMVXFile dir = (JMVXFile) p.getParent().toFile();
        return dir.$JMVX$mkdirs();
    }

}
