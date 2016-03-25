package org.cobbzilla.s3s3mirror.store.local.job;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.KeyDeleteJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.s3.S3ClientService;
import org.cobbzilla.s3s3mirror.store.s3.S3FileListing;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;
import org.slf4j.Logger;

import java.io.File;

@Slf4j
public class LocalKeyDeleteJob extends KeyDeleteJob {

    @Override public Logger getLog() { return log; }

    private AmazonS3Client s3client;

    public LocalKeyDeleteJob(MirrorContext context, FileSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
        s3client = S3ClientService.getS3Client(context.getOptions());
    }

    @Override protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return S3FileListing.buildSummary(key, S3FileStore.getObjectMetadata(bucket, key, context, s3client));
//        return LocalFileListing.buildSummary(LocalFileStore.getFile(bucket, key));
    }

    @Override protected boolean deleteFile(String bucket, String key) throws Exception {
        final MirrorOptions options = context.getOptions();

        final File destFile = LocalFileStore.getFileAndCreateParent(options.getDestinationBucket(), key);

        return destFile.delete();
    }

}
