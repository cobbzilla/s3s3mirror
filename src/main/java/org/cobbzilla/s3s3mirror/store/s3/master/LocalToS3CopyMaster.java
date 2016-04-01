package org.cobbzilla.s3s3mirror.store.s3.master;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.local.job.LocalS3KeyLinkJob;
import org.cobbzilla.s3s3mirror.store.s3.job.S3KeyUploadJob;
import org.cobbzilla.s3s3mirror.store.s3.job.S3MultipartUploadJob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public class LocalToS3CopyMaster extends S3Master {

    public LocalToS3CopyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(context, workQueue, executorService);
    }

    @Override public FileStore getListSource() { return LocalFileStore.instance; }

    @Override
    protected S3KeyUploadJob getTask(FileSummary summary) {
        if (summary == null) return null;
        if (summary.isSymlink()) {
            return new LocalS3KeyLinkJob(s3client, context, summary, notifyLock);
        }
        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return new S3MultipartUploadJob(s3client, context, summary, notifyLock);
        }
        return new S3KeyUploadJob(s3client, context, summary, notifyLock);
    }

}
