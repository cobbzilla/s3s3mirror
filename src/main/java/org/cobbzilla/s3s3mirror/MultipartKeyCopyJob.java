package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.job.S3KeyCopyJob;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MultipartKeyCopyJob extends S3KeyCopyJob {

    public MultipartKeyCopyJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        super(client, context, summary, notifyLock, comparisonStrategy);
    }

    @Override
    public boolean copyFile () throws Exception {

        final MirrorOptions options = context.getOptions();
        final String sourceBucket = options.getSourceBucket();
        final String destBucket = options.getDestinationBucket();
        final List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        final int maxPartRetries = options.getMaxRetries();

        final String key = summary.getKey();
        final long objectSize = summary.getSize();
        final String keydest = getKeyDestination();

        if (options.isVerbose()) log.info("Initiating multipart upload request for " + key);

        final InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(destBucket, keydest)
                .withObjectMetadata(getObjectMetadata(sourceBucket, key));

        options.apply(initiateRequest);

        if (options.isCrossAccountCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(getAccessControlList(options, key));
        }

        final InitiateMultipartUploadResult initResult = s3client.initiateMultipartUpload(initiateRequest);

        long partSize = options.getUploadPartSize();
        long bytePosition = 0;

        for (int i = 1; bytePosition < objectSize; i++) {

            final long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            final String infoMessage = "copying : " + bytePosition + " to " + lastByte;
            if (options.isVerbose()) log.info(infoMessage);

            final CopyPartRequest copyRequest = new CopyPartRequest()
                    .withDestinationBucketName(destBucket)
                    .withDestinationKey(keydest)
                    .withSourceBucketName(sourceBucket)
                    .withSourceKey(key)
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(i);

            for (int tries = 1; tries <= maxPartRetries; tries++) {
                try {
                    if (options.isVerbose()) log.info("try :" + tries);
                    context.getStats().s3copyCount.incrementAndGet();
                    CopyPartResult copyPartResult = s3client.copyPart(copyRequest);
                    copyResponses.add(copyPartResult);
                    if (options.isVerbose()) log.info("completed " + infoMessage);
                    break;
                } catch (Exception e) {
                    if (tries == maxPartRetries) {
                        s3client.abortMultipartUpload(new AbortMultipartUploadRequest(
                                destBucket, keydest, initResult.getUploadId()));
                        log.error("Exception while doing multipart copy", e);
                        return false;
                    }
                }
            }

            bytePosition += partSize;
        }

        final CompleteMultipartUploadRequest completeRequest
                = new CompleteMultipartUploadRequest(destBucket, keydest, initResult.getUploadId(), getETags(copyResponses));
        s3client.completeMultipartUpload(completeRequest);

        if (options.isVerbose()) log.info("completed multipart request for : " + key);

        context.getStats().bytesCopied.addAndGet(objectSize);
        return true;
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

}
