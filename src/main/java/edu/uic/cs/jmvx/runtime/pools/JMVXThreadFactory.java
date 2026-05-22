package edu.uic.cs.jmvx.runtime.pools;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class JMVXThreadFactory implements ThreadFactory {

    private final Optional<ThreadFactory> parent;

    public JMVXThreadFactory(Optional<ThreadFactory> parent) {
        this.parent = parent;
    }

    public static class JMVXPoolThread extends Thread {
        private Thread parent;

        public JMVXPoolThread(Thread parent) {
            this.parent = parent;
        }

        @Override
        public void interrupt() {
            parent.interrupt();
        }

        @Override
        public boolean isInterrupted() {
            return parent.isInterrupted();
        }

        @Override
        public void destroy() {
            parent.destroy();
        }

        @Override
        public int countStackFrames() {
            return parent.countStackFrames();
        }

        @Override
        public String toString() {
            return parent.toString();
        }

        @Override
        public ClassLoader getContextClassLoader() {
            return parent.getContextClassLoader();
        }

        @Override
        public void setContextClassLoader(ClassLoader cl) {
            parent.setContextClassLoader(cl);
        }

        @Override
        public StackTraceElement[] getStackTrace() {
            return parent.getStackTrace();
        }

        @Override
        public long getId() {
            return parent.getId();
        }

        @Override
        public State getState() {
            return parent.getState();
        }

        @Override
        public UncaughtExceptionHandler getUncaughtExceptionHandler() {
            return parent.getUncaughtExceptionHandler();
        }

        @Override
        public void setUncaughtExceptionHandler(UncaughtExceptionHandler eh) {
            parent.setUncaughtExceptionHandler(eh);
        }

        @Override
        public void run() {
            /* have to add these calls manually call this manually
             * because JMVXPoolThread is not instrumented! */
            try {
                JMVXRuntime.run(this); //sets toplevel
                parent.run(); //instrumented
            }catch (TerminateException e){
                //pass, this only comes from us as a failsafe to prevent blocking indefinitely
            } finally {
                JMVXRuntime.exitThread(this);
            }
        }
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread toWrap = parent.orElse(Executors.defaultThreadFactory()).newThread(r);
        if(toWrap == null){
            throw new RuntimeException("Could not create enough threads");
        }
        return new JMVXPoolThread(toWrap);
    }

    //used by the follower to terminate the thread based on a message received by the leader
    public static class TerminateException extends RuntimeException {
        public TerminateException(String message) {
            super(message);
        }
    }
}
