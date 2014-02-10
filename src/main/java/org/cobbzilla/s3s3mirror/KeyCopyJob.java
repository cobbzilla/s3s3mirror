package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class KeyCopyJob extends KeyJob {

    private String keydest;

    public KeyCopyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);

        keydest = summary.getKey();
        final MirrorOptions options = context.getOptions();
        if (options.hasDestPrefix()) {
            keydest = keydest.substring(options.getPrefixLength());
            keydest = options.getDestPrefix() + keydest;
        }
    }

    @Override public Logger getLog() { return log; }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) return;
            final ObjectMetadata sourceMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
            final AccessControlList objectAcl = getAccessControlList(options, key);

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + keydest);
            } else {
                boolean copiedOK = false;
                if (shouldMultiPartCopy()) {
                    multiPartCopy(sourceMetadata, objectAcl);
                    copiedOK = true;
                } else {
                    for (int tries = 0; tries < maxRetries; tries++) {
                        if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);
                        final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest);
                        request.setNewObjectMetadata(sourceMetadata);
                        request.setAccessControlList(objectAcl);

                        try {
                            stats.s3copyCount.incrementAndGet();
                            client.copyObject(request);
                            stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                            copiedOK = true;
                            if (verbose)
                                log.info("successfully copied (on try #" + tries + "): " + key + " to: " + keydest);
                        } catch (AmazonS3Exception s3e) {
                            log.error("s3 exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + s3e);

                        } catch (Exception e) {
                            log.error("unexpected exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
                        }
                        if (copiedOK) break;
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            log.error("interrupted while waiting to retry key: " + key);
                            break;
                        }
                    }
                }
                if (copiedOK) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            log.error("error copying key: "+key+": "+e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("done with "+key);
        }
    }

    private void multiPartCopy(ObjectMetadata metadata, AccessControlList acl) throws Exception {
        long objectSize = summary.getSize();
        MirrorOptions options = context.getOptions();
        String sourceBucketName = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        String targetBucketName = options.getDestinationBucket();
        List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        if (options.isVerbose()) log.info("Initiating multipart upload request for " + summary.getKey());
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(targetBucketName, keydest)
                .withAccessControlList(acl)
                .withObjectMetadata(metadata);

        InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initiateRequest);

        long partSize = options.getUploadPartSize();
        long bytePosition = 0;

        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            if (options.isVerbose()) log.info("copying : " + bytePosition + " to " + lastByte);
            CopyPartRequest copyRequest = new CopyPartRequest()
                    .withDestinationBucketName(targetBucketName)
                    .withDestinationKey(keydest)
                    .withSourceBucketName(sourceBucketName)
                    .withSourceKey(summary.getKey())
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(i);

            for (int tries = 1; tries <= maxPartRetries; tries++) {
                try {
                    log.info("try :" + tries);
                    context.getStats().s3copyCount.incrementAndGet();
                    CopyPartResult copyPartResult = client.copyPart(copyRequest);
                    copyResponses.add(copyPartResult);
                    break;
                } catch (Exception e) {
                    if (tries == maxPartRetries) {
                        client.abortMultipartUpload(new AbortMultipartUploadRequest(
                                targetBucketName, keydest, initResult.getUploadId()));
                        throw new RuntimeException("Exception while doing multipart copy", e);
                    }
                }
            }
            bytePosition += partSize;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        client.completeMultipartUpload(completeRequest);
        context.getStats().bytesCopied.addAndGet(objectSize);
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

    private boolean shouldMultiPartCopy() {
        return summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE;
    }

    private boolean shouldTransfer() {

        final MirrorOptions options = context.getOptions();

        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+"), not copying");
                    return false;
                }
            }
        }

        final KeyFingerprint sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());

        final ObjectMetadata metadata;
        try {
            metadata = getObjectMetadata(options.getDestinationBucket(), keydest, options);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in destination bucket (will copy): "+ keydest);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return metadata.getContentLength() != summary.getSize();
        }
        final KeyFingerprint destFingerprint = new KeyFingerprint(metadata.getContentLength(), metadata.getETag());

        final boolean objectChanged = !sourceFingerprint.equals(destFingerprint);
        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: "+ key);

        return objectChanged;
    }
}
