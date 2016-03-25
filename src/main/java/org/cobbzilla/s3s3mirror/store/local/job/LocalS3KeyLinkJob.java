package org.cobbzilla.s3s3mirror.store.local.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.input.NullInputStream;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.job.S3KeyUploadJob;

import java.io.InputStream;

public class LocalS3KeyLinkJob extends S3KeyUploadJob {

    private static final InputStream DEV_NULL = new NullInputStream(0);

    public LocalS3KeyLinkJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override protected boolean copyFile() throws Exception {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();

        final ObjectMetadata redirectMetadata = new ObjectMetadata();
        redirectMetadata.setHeader("x-amz-website-redirect-location", summary.getKey());
        final PutObjectRequest request = new PutObjectRequest(options.getDestinationBucket(),
                                                              getKeyDestination().replace("\\", "/"),
                                                              DEV_NULL,
                                                              redirectMetadata);
        options.apply(request);

        stats.s3putCount.incrementAndGet();
        s3client.putObject(request);
        // don't increment bytes -- this is a symlink
        // stats.bytesUploaded.addAndGet(srcFile.length());
        return true;
    }
}
