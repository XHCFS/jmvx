package edu.uic.cs.jmvx.runtime.strategy;

/**
 * This fails, as it should
 * The paired recorder does not log enough sources of nondeterminism
 * Nonetheless the code is here in case we need it
 */
public class ReplayerSyncOnly extends SyncOnly<Replayer> {
    public ReplayerSyncOnly() {
        super(new ReplayerWithPools());
    }
}
