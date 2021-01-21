package org.cobbzilla.s3s3mirror.stats;

import java.time.Duration;

public interface StatusListener {
    Duration getUpdateInterval();
    void provideStatus(MirrorStats mirrorStats);
}
