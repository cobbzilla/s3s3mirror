package org.cobbzilla.s3s3mirror.store.s3.master;

import org.cobbzilla.s3s3mirror.KeyDeleteJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.s3.job.LocalKeyDeleteJob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Deletes locally if the file does not exist on S3
 */
public class S3ToLocalDeleteMaster extends S3DeleteMaster {

    public S3ToLocalDeleteMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        super(context, workQueue, executor);
    }

    @Override public FileStore getListSource() { return LocalFileStore.instance; }

    @Override protected KeyDeleteJob getTask(FileSummary summary) {
        return new LocalKeyDeleteJob(context, summary, notifyLock);
    }
}
