package edu.uic.cs.jmvx.runtime.util;

public class UIDedObject {

    private volatile Object lastOwner = null;

    public Object getLastOwner() {
        return lastOwner;
    }

    public void setLastOwner(Object lastOwner) {
        this.lastOwner = lastOwner;
    }

    public boolean sameOwner(Object thisOwner){
        return lastOwner == thisOwner;
    }

    /* For RecorderRTOArtifactTracer, if distances between artifacting is needed

    private long lastSequenceNumber = 0;
    public long getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    public void setLastSequenceNumber(long lastSequenceNumber) {
        this.lastSequenceNumber = lastSequenceNumber;
    }*/
}
