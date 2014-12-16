package org.cobbzilla.s3s3mirror.store.s3.job;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileListing;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.s3.S3ClientService;
import org.slf4j.Logger;

/**
 * Deletes from S3 if the file is not found locally
 */
@Slf4j
public class LocalS3KeyDeleteJob extends S3KeyDeleteJob {

    @Override public Logger getLog() { return log; }

    public LocalS3KeyDeleteJob(MirrorContext context, FileSummary summary, Object notifyLock) {
        super(S3ClientService.getS3Client(context.getOptions()), context, summary, notifyLock);
    }

    @Override protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return LocalFileListing.buildSummary(LocalFileStore.getFile(bucket, key), bucket);
    }
}
