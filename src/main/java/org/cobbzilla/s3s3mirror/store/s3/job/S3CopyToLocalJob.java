package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.local.job.LocalKeyCopyJob;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Copies S3->local if the local file does not exist or is different than the object in S3
 */
@Slf4j
public class S3CopyToLocalJob extends LocalKeyCopyJob {

    @Override public Logger getLog() { return log; }

    public S3CopyToLocalJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
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
        IOUtils.copy(s3client.getObject(request).getObjectContent(), out);
        stats.bytesCopied.addAndGet(destFile.length());

        return true;
    }

}
