package org.cobbzilla.s3s3mirror.comparisonstrategies;

public enum SyncStrategy {
    SIZE,
    SIZE_ETAG,
    SIZE_SHA256,
    SIZE_LAST_MODIFIED,
    AUTO
}
