package edu.uic.cs.jmvx.runtime;

/** JNI replacement for sun.misc.Unsafe.monitorEnter/monitorExit. */
public final class JMVXMonitor {
    static {
        // System.loadLibrary searches sun.boot.library.path for boot classes, so load by absolute path
        System.load(System.getProperty("jmvx.monitorlib"));
    }
    public static native void enter(Object o);
    public static native void exit(Object o);
}
