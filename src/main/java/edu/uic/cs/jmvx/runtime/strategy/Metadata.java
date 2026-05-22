package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.runtime.JMVXInputStream;

public class Metadata {
    public byte[] buffer;
    public int offset;
    public int length;
    public boolean isOffsetPresent;

    // from represents which kind of method this metadata is coming from
    // There are 3 cases: 1) a read, 2) a write or 3) anything else
    public DivergenceSource source;

    // original buffer is the buffer that is coming from the leader
    // Similarly for originalOffset and originalLength
    public byte[] originalBuffer;
    public int originalOffset;
    public int originalLength;

    public JMVXInputStream is;

    Metadata() {
    }

    Metadata(byte[] buffer, int length, int offset) {
        this.buffer = buffer;
        this.length = length;
        this.offset = offset;
    }

    Metadata(byte[] buffer, int length, int offset, DivergenceSource source) {
        this(buffer, length, offset);
        this.source = source;
    }
}
