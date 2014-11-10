package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.s3.S3ClientService;
import org.cobbzilla.s3s3mirror.store.s3.S3FileListing;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;

import java.io.File;

/**
 * Uploads local->S3 if the object on S3 does not exist or is different than the local file.
 */
@Slf4j
public class S3KeyUploadJob extends LocalKeyCopyJob {

    protected AmazonS3Client s3client;

    public S3KeyUploadJob(MirrorContext context, FileSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
        s3client = S3ClientService.getS3Client(context.getOptions());
    }

    @Override protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return S3FileListing.buildSummary(key, S3FileStore.getObjectMetadata(bucket, key, context, s3client));
    }

    @Override protected boolean copyFile() throws Exception {

        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();

        final File srcFile = LocalFileStore.getFile(options.getSourceBucket(), summary.getKey());
        final PutObjectRequest request = new PutObjectRequest(options.getDestinationBucket(), getKeyDestination(), srcFile);
        options.apply(request);

        stats.s3putCount.incrementAndGet();
        s3client.putObject(request);
        stats.bytesUploaded.addAndGet(srcFile.length());
        return true;
    }
}
