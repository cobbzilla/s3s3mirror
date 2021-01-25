package org.cobbzilla.s3s3mirror.store.local.job;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.KeyCopyJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileListing;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.slf4j.Logger;

@Slf4j
public abstract class LocalKeyCopyJob extends KeyCopyJob {

    @Override public Logger getLog() { return log; }

    public LocalKeyCopyJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        super(client, context, summary, notifyLock, comparisonStrategy);
    }

    @Override
    protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return LocalFileListing.buildSummary(LocalFileStore.getFile(bucket, key), bucket);
    }

}
