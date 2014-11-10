package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.KeyDeleteJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.S3FileListing;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;

@Slf4j
public class S3KeyDeleteJob extends KeyDeleteJob {

    private final AmazonS3Client s3client;

    public S3KeyDeleteJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);
        this.s3client = client;
    }

    @Override protected boolean deleteFile(String bucket, String key) {
        context.getStats().s3deleteCount.incrementAndGet();
        final DeleteObjectRequest request = new DeleteObjectRequest(bucket, key);
        s3client.deleteObject(request);
        return true;
    }

    @Override protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return S3FileListing.buildSummary(key, S3FileStore.getObjectMetadata(bucket, key, context, s3client));
    }

}
