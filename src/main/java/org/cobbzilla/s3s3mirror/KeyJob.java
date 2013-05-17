package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyJob implements Runnable {

    private final AmazonS3Client client;
    private final MirrorOptions options;
    private final S3ObjectSummary summary;
    private final Object notifyLock;

    public KeyJob(AmazonS3Client client, MirrorOptions options, S3ObjectSummary summary, Object notifyLock) {
        this.client = client;
        this.options = options;
        this.summary = summary;
        this.notifyLock = notifyLock;
    }

    @Override public String toString() { return summary.getKey(); }

    @Override
    public void run() {
        final boolean verbose = options.isVerbose();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) {
                if (verbose) {
                    log.info("Destination file is same as source, not copying: "+ key);
                }
                return;
            }

            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), key);

            final ObjectMetadata sourceMetadata = client.getObjectMetadata(options.getSourceBucket(), key);
            request.setNewObjectMetadata(sourceMetadata);

            final AccessControlList objectAcl = client.getObjectAcl(options.getSourceBucket(), key);
            request.setAccessControlList(objectAcl);

            if (options.isDryRun()) {
                log.info("Would have copied "+ key +" to destination");
            } else {
                log.info("copying: "+key);
                try {
                    client.copyObject(request);
                } catch (Exception e) {
                    log.error("error copying "+key+": "+e);
                }
            }

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("done with "+key);
        }
    }

    private boolean shouldTransfer() {

        final KeyFingerprint sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());

        final ObjectMetadata metadata;
        try {
            metadata = client.getObjectMetadata(options.getDestinationBucket(), summary.getKey());
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (options.isVerbose()) log.info("Key not found in destination bucket (will copy): "+summary.getKey());
                return true;
            } else {
                log.info("Error getting metadata for "+options.getDestinationBucket()+"/"+summary.getKey()+" (not copying): "+e);
                return false;
            }
        } catch (Exception e) {
            log.info("Error getting metadata for "+options.getDestinationBucket()+"/"+summary.getKey()+" (not copying): "+e);
            return false;
        }

        final KeyFingerprint destFingerprint = new KeyFingerprint(metadata.getContentLength(), metadata.getETag());

        return !sourceFingerprint.equals(destFingerprint);
    }


}
