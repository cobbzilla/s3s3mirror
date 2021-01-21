package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.KeyCopyJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.stats.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.S3FileListing;
import org.slf4j.Logger;

@Slf4j
public class S3KeyCopyJob extends KeyCopyJob {

    @Override public Logger getLog() { return log; }

    public S3KeyCopyJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override public String toString() { return summary.getKey(); }

    protected AccessControlList getAccessControlList(MirrorOptions options, String key) throws Exception {
        Exception ex = null;
        for (int tries=0; tries<options.getMaxRetries(); tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                return s3client.getObjectAcl(options.getSourceBucket(), key);

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectAcl(" + key + ") failed (try #" + tries + "), giving up");
                        break;
                    } else {
                        log.warn("getObjectAcl(" + key + ") failed (try #" + tries + "), retrying...");
                    }
                }
            }
        }
        throw ex;
    }

    @Override
    protected FileSummary getMetadata(String bucket, String key) throws Exception {
        final ObjectMetadata metadata = getObjectMetadata(bucket, key);
        return metadata == null ? null : S3FileListing.buildSummary(key, metadata);
    }

    @Override
    protected boolean copyFile() throws Exception {

        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final String key = summary.getKey();
        final String keydest = getKeyDestination();

        final ObjectMetadata sourceMetadata = getObjectMetadata(options.getSourceBucket(), key);

        final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest);
        request.setNewObjectMetadata(sourceMetadata);
        options.apply(request);

        if (options.isCrossAccountCopy()) {
            request.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            final AccessControlList objectAcl = getAccessControlList(options, key);
            request.setAccessControlList(objectAcl);
        }

        stats.s3copyCount.incrementAndGet();
        s3client.copyObject(request);
        stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
        return true;
    }

}
