package org.cobbzilla.s3s3mirror.store.s3.master;

import org.cobbzilla.s3s3mirror.KeyDeleteJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.job.S3KeyDeleteJob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class S3DeleteMaster extends S3Master {

    public S3DeleteMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        super(context, workQueue, executor);
    }

    @Override protected KeyDeleteJob getTask(FileSummary summary) {
        return new S3KeyDeleteJob(s3client, context, summary, notifyLock);
    }

    @Override protected String getPrefix(MirrorOptions options) {
        return options.hasDestPrefix() ? options.getDestPrefix() : "";
    }

    @Override protected String getBucket(MirrorOptions options) { return options.getDestinationBucket(); }

}
