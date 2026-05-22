package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.runtime.JMVXClassLoader;
import edu.uic.cs.jmvx.runtime.JMVXJarFile;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.pools.JMVXJob;

import java.io.IOException;

public class FollowerRecorder extends ReentrantStrategy<FollowerRecorderImpl, Recorder> {

    private static volatile boolean staticInit = false;
    int clinit = 0;
    int loading = 0;

    /* Important detail:
     * This class will be wrapped by a ReentrantStrategy
     * A call to `JMVXRuntime.enter/exit` is for the Reentrant (eg to skip ops related to flushing)
     * A call to `enter/exit` is for switching between recorder and follower w.r.t. loading/clinits/zips
     */

    public FollowerRecorder() {
        super(new FollowerRecorderImpl(), new Recorder());
        if(!staticInit) {
            synchronized (FollowerRecorder.class) {
                if(!staticInit) {
                    passthrough.initStaticStreams();
                    staticInit = true;
                }
            }
        }
    }

    @Override
    public void enter() {
        if(loading + clinit > 0){
            inUse = passthrough;
        }
    }

    @Override
    public void exit() {
        if(loading + clinit == 0){
            inUse = strategy;
        }
    }

    @Override
    public void clinitStart(String cls) {
        clinit++;
        enter();
        inUse.clinitStart(cls);
    }

    @Override
    public void clinitEnd(String cls) {
        try {
            inUse.clinitEnd(cls);
        }finally {
            clinit--;
            exit();
        }
    }

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name) throws ClassNotFoundException {
        return this.loadClass(loader, name, true);
    }

    @Override
    public Class<?> loadClass(JMVXClassLoader loader, String name, boolean resolve) throws ClassNotFoundException {
        try {
            loading++;
            enter();
            return inUse.loadClass(loader, name, resolve);
        }finally {
            loading--;
            exit();
        }
    }

    @Override
    public void main() {
        strategy.main();
        passthrough.main(strategy.vectorClockIndex, false);
        passthrough.toCoordinator = strategy.toLogFile;
    }

    @Override
    public void exitLogic() {
        try{
            JMVXRuntime.enter();
            passthrough.exitLogic();
        }finally {
            JMVXRuntime.exit();
        }
        strategy.exitLogic();
    }

    @Override
    public void systemExit(int status) {
        try {
            JMVXRuntime.enter();
            passthrough.flushData(true);
        }finally {
            JMVXRuntime.exit();
        }
        strategy.systemExit(status);
    }

    /* hack for now
    * This can get called outside of class loading */
    @Override
    public String[] getMetaInfEntryNames(JMVXJarFile jarfile) {
        return passthrough.getMetaInfEntryNames(jarfile);
    }

    @Override
    public void initIDs() {
        passthrough.initIDs();
    }

    @Override
    public long getEntry(long jzfile, byte[] name, boolean addSlash) {
        return passthrough.getEntry(jzfile, name, addSlash);
    }

    @Override
    public void freeEntry(long jzfile, long jzentry) {
        passthrough.freeEntry(jzfile, jzentry);
    }

    @Override
    public long getNextEntry(long jzfile, int i) {
        return passthrough.getNextEntry(jzfile, i);
    }

    @Override
    public void close(long jzfile) {
        passthrough.close(jzfile);
    }

    @Override
    public long open(String name, int mode, long lastModified, boolean usemmap) throws IOException {
        return passthrough.open(name, mode, lastModified, usemmap);
    }

    @Override
    public int getTotal(long jzfile) {
        return passthrough.getTotal(jzfile);
    }

    @Override
    public boolean startsWithLOC(long jzfile) {
        return passthrough.startsWithLOC(jzfile);
    }

    @Override
    public int read(long jzfile, long jzentry, long pos, byte[] b, int off, int len) {
        return passthrough.read(jzfile, jzentry, pos, b, off, len);
    }

    @Override
    public long getEntryTime(long jzentry) {
        return passthrough.getEntryTime(jzentry);
    }

    @Override
    public long getEntryCrc(long jzentry) {
        return passthrough.getEntryCrc(jzentry);
    }

    @Override
    public long getEntryCSize(long jzentry) {
        return passthrough.getEntryCSize(jzentry);
    }

    @Override
    public long getEntrySize(long jzentry) {
        return passthrough.getEntrySize(jzentry);
    }

    @Override
    public int getEntryMethod(long jzentry) {
        return passthrough.getEntryMethod(jzentry);
    }

    @Override
    public int getEntryFlag(long jzentry) {
        return passthrough.getEntryFlag(jzentry);
    }

    @Override
    public byte[] getCommentBytes(long jzfile) {
        return passthrough.getCommentBytes(jzfile);
    }

    @Override
    public byte[] getEntryBytes(long jzentry, int type) {
        return passthrough.getEntryBytes(jzentry, type);
    }

    @Override
    public String getZipMessage(long jzfile) {
        return passthrough.getZipMessage(jzfile);
    }

    @Override
    public void beforeExecute(Thread t, JMVXJob r) {
        //do we want the recorder to execute this? And then set the follower's log stream?
        //that would let us decide between pool recording strategies (Recorder vs RecorderWithPools)
        strategy.beforeExecute(t, r);
        //stream reference to restore after a class load changes!
        passthrough.toCoordinator = strategy.toLogFile;
    }

    @Override
    public void afterExecute(JMVXJob r, Throwable t) {
        strategy.afterExecute(r, t);
        //stream reference to restore after a class load changes!
        passthrough.toCoordinator = strategy.toLogFile;
    }
}
