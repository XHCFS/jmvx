package edu.uic.cs.jmvx.runtime.strategy;

/**
 * Record only synchronization primitives
 * Useful to understand how much cost we pay handling IO vs synchronization
 * However, this mode still requires IO events to be instrumented.
 * The Recorder makes assumptions about being able to cast File to JMVXFile when setting up the recording
 * Therefore, without the instrumentation, the Recorder crashes.
 */
public class RecorderSyncOnly extends SyncOnly<Recorder> {
    public RecorderSyncOnly() {
        super(new RecorderWithPools());
    }
}
