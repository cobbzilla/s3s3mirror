package org.cobbzilla.s3s3mirror.store.s3.master;

import org.cobbzilla.s3s3mirror.KeyCopyJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.job.S3CopyToLocalJob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class S3ToLocalCopyMaster extends S3Master {

    public S3ToLocalCopyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        super(context, workQueue, executor);
    }

    @Override protected KeyCopyJob getTask(FileSummary summary) {
        return new S3CopyToLocalJob(s3client, context, summary, notifyLock);
    }
}
