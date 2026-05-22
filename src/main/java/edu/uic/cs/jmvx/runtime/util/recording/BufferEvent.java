package edu.uic.cs.jmvx.runtime.util.recording;

import edu.uic.cs.jmvx.runtime.util.OptimizedEvent;

import java.io.Serializable;

/*default*/ public class BufferEvent implements Serializable {
    private static final long serialVersionUID = -8166362716492380175L;
    public final int ret;
    public final byte[] b;
    public final int off, len;
    public final OptimizedEvent tag; //could be READ, WRITE, RANDOM_ACCESS_READ, RANDOM_ACCESS_WRITE

    public BufferEvent(OptimizedEvent tag, byte[] b, int off, int len, int ret) {
        this.tag = tag;
        this.ret = ret;
        this.b = b;
        this.off = off;
        this.len = len;
    }
}
