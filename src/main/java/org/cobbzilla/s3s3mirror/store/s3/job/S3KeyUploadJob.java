package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.stats.MirrorStats;
import org.cobbzilla.s3s3mirror.Sha256;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.local.job.LocalKeyCopyJob;
import org.cobbzilla.s3s3mirror.store.s3.S3FileListing;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;
import org.slf4j.Logger;

import java.io.File;

/**
 * Uploads local->S3 if the object on S3 does not exist or is different than the local file.
 */
@Slf4j
public class S3KeyUploadJob extends LocalKeyCopyJob {

    @Override public Logger getLog() { return log; }

    public S3KeyUploadJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        super(client, context, summary, notifyLock, comparisonStrategy);
    }

    protected FileSummary getMetadata(String bucket, String key) throws Exception {
        return S3FileListing.buildSummary(key, S3FileStore.getObjectMetadata(bucket, key, context, s3client));
    }

    @Override protected boolean copyFile() throws Exception {

        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();

        final File srcFile = LocalFileStore.getFile(options.getSourceBucket(), summary.getKey());
        final PutObjectRequest request = new PutObjectRequest(options.getDestinationBucket(), getKeyDestination().replace("\\", "/"), srcFile);
        request.setMetadata(Sha256.getS3MetadataWithHash(srcFile));
        options.apply(request);

        stats.s3putCount.incrementAndGet();
        s3client.putObject(request);
        stats.bytesUploaded.addAndGet(srcFile.length());
        return true;
    }

}
