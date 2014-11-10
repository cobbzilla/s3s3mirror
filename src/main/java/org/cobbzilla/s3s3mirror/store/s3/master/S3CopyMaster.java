package org.cobbzilla.s3s3mirror.store.s3.master;

import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.MultipartKeyCopyJob;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.job.S3KeyCopyJob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class S3CopyMaster extends S3Master {

    public S3CopyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(context, workQueue, executorService);
    }

    @Override
    protected S3KeyCopyJob getTask(FileSummary summary) {
        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return new MultipartKeyCopyJob(s3client, context, summary, notifyLock);
        }
        return new S3KeyCopyJob(s3client, context, summary, notifyLock);
    }

}
