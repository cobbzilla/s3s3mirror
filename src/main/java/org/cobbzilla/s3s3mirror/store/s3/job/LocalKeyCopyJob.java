package org.cobbzilla.s3s3mirror.store.s3.job;

import org.cobbzilla.s3s3mirror.KeyCopyJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileListing;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;

public abstract class LocalKeyCopyJob extends KeyCopyJob {

    public LocalKeyCopyJob(MirrorContext context, FileSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
    }

    @Override
    protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return LocalFileListing.buildSummary(LocalFileStore.getFile(bucket, key), bucket);
    }

}
