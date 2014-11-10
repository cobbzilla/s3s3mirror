package org.cobbzilla.s3s3mirror.store.s3.master;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.Getter;
import org.cobbzilla.s3s3mirror.KeyMaster;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class S3Master extends KeyMaster {

    protected final AmazonS3Client s3client;
    @Getter private final FileStore listSource;

    public S3Master(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(context, workQueue, executorService);
        listSource = new S3FileStore(context.getOptions());
        s3client = ((S3FileStore) listSource).getS3client();
    }

    @Override protected String getPrefix(MirrorOptions options) { return options.getPrefix(); }

    @Override protected String getBucket(MirrorOptions options) { return options.getSourceBucket(); }

}
