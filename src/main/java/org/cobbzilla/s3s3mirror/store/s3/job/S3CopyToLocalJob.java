package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.MirrorStats;
import org.cobbzilla.s3s3mirror.Sha256;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.local.job.LocalKeyCopyJob;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.cobbzilla.s3s3mirror.Sha256.S3S3_SHA256;

/**
 * Copies S3->local if the local file does not exist or is different than the object in S3
 */
@Slf4j
public class S3CopyToLocalJob extends LocalKeyCopyJob {

    @Override public Logger getLog() { return log; }

    public S3CopyToLocalJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        super(client, context, summary, notifyLock, comparisonStrategy);
        this.s3client = client;
    }

    @Override
    protected boolean copyFile() throws Exception {

        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final String key = summary.getKey();
        final String keydest = getKeyDestination();

        final GetObjectRequest request = new GetObjectRequest(options.getSourceBucket(), key);
        final File destFile = LocalFileStore.getFileAndCreateParent(options.getDestinationBucket(), keydest);

        @Cleanup final OutputStream out = new FileOutputStream(destFile);
        stats.s3getCount.incrementAndGet();
        final S3Object object = s3client.getObject(request);
        IOUtils.copy(object.getObjectContent(), out);
        stats.bytesCopied.addAndGet(destFile.length());

        // If the source object does not have a shasum in its user metadata, add one now
        final ObjectMetadata objectMetadata = object.getObjectMetadata();
        final Map<String, String> userMeta = objectMetadata.getUserMetadata();
        if (userMeta == null || !userMeta.containsKey(S3S3_SHA256)) {
            final Map<String, String> meta = userMeta == null ? new HashMap<String, String>() : userMeta;
            meta.putAll(Sha256.userMetaWithHash(destFile));
            final CopyObjectRequest updateMeta = new CopyObjectRequest(
                    options.getSourceBucket(), key,
                    options.getSourceBucket(), key)
                    .withNewObjectMetadata(objectMetadata);
            options.apply(updateMeta);
            stats.s3copyCount.incrementAndGet();
            s3client.copyObject(updateMeta);
        }

        return true;
    }

}
