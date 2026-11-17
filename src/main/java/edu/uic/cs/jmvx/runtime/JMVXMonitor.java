package edu.uic.cs.jmvx.runtime;

public final class JMVXMonitor {
    static {
        System.load(System.getProperty("jmvx.monitorlib"));
    }
    public static native void enter(Object o);
    public static native void exit(Object o);
}
