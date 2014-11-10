package org.cobbzilla.s3s3mirror.store.s3.master;

import org.cobbzilla.s3s3mirror.KeyJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.job.LocalS3KeyDeleteJob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Deletes from S3 if the file does not exist locally
 */
public class LocalToS3DeleteMaster extends S3Master {

    public LocalToS3DeleteMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        super(context, workQueue, executor);
    }

    protected String getPrefix(MirrorOptions options) { return options.getDestPrefix(); }
    protected String getBucket(MirrorOptions options) { return options.getDestinationBucket(); }

    @Override
    protected KeyJob getTask(FileSummary summary) {
        return new LocalS3KeyDeleteJob(context, summary, notifyLock);
    }
}
