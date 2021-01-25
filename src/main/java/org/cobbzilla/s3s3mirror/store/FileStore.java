package org.cobbzilla.s3s3mirror.store;

import org.cobbzilla.s3s3mirror.stats.MirrorStats;

public interface FileStore {

    public FileListing listObjects(ListRequest request, MirrorStats stats);

    public FileListing listNextBatch(FileListing listing, MirrorStats stats);
}
