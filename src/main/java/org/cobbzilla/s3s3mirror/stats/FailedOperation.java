package org.cobbzilla.s3s3mirror.stats;

import lombok.Value;

@Value
public class FailedOperation {
    private final String source;
    private final String destination;
}
