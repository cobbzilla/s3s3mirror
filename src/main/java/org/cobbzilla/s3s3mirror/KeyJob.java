package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
public class KeyJob implements Runnable {

    private final AmazonS3Client client;
    private final MirrorContext context;
    private final S3ObjectSummary summary;
    private final Object notifyLock;

    public KeyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        this.client = client;
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;
    }

    @Override public String toString() { return summary.getKey(); }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) {
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
                    context.getStats().objectsCopied++;
                } catch (Exception e) {
                    log.error("error copying "+key+": "+e);
                    context.getStats().copyErrors++;
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

        final MirrorOptions options = context.getOptions();

        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) {
                    log.info("No Last-Modified header for key: " + key);
                }
            } else {
                if (options.getNowTime() - lastModified.getTime() > options.getCtimeMillis()) {
                    if (verbose) {
                        log.info("key is older than "+options.getCtime()+" days (not copying)");
                    }
                    return false;
                }
            }
        }

        final KeyFingerprint sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());

        final ObjectMetadata metadata;
        try {
            metadata = client.getObjectMetadata(options.getDestinationBucket(), key);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in destination bucket (will copy): "+ key);
                return true;
            } else {
                log.info("Error getting metadata for "+options.getDestinationBucket()+"/"+ key +" (not copying): "+e);
                return false;
            }
        } catch (Exception e) {
            log.info("Error getting metadata for "+options.getDestinationBucket()+"/"+ key +" (not copying): "+e);
            return false;
        }

        final KeyFingerprint destFingerprint = new KeyFingerprint(metadata.getContentLength(), metadata.getETag());

        final boolean objectChanged = !sourceFingerprint.equals(destFingerprint);
        if (verbose) {
            if (!objectChanged) log.info("Destination file is same as source, not copying: "+ key);
        }
        return objectChanged;
    }


}
