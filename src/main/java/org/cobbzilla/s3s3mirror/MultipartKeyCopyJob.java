package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static org.cobbzilla.s3s3mirror.MirrorConstants.*;

@Slf4j
public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override
    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        long objectSize = summary.getSize();
        MirrorOptions options = context.getOptions();
        String sourceBucketName = options.getSourceBucket();
        int maxPartRetries = options.getMaxRetries();
        String targetBucketName = options.getDestinationBucket();
        List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        if (options.isVerbose()) {
            log.info("Initiating multipart upload request for " + summary.getKey());
        }
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(targetBucketName, keydest)
                .withObjectMetadata(sourceMetadata);

        if (options.isCrossAccountCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(objectAcl);
        }

        InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initiateRequest);

        final long optionsUploadPartSize = options.getUploadPartSize();
        long partSize = MirrorOptions.MINIMUM_PART_SIZE;
        
        if (optionsUploadPartSize == MirrorOptions.DEFAULT_PART_SIZE ) {
            final String eTag = summary.getETag();
            final int eTagParts = Integer.parseInt(eTag.substring(eTag.indexOf(MirrorOptions.ETAG_MULTIPART_DELIMITER.toString()) + 1,eTag.length()));

            long computedPartSize = MirrorConstants.MB;
            long minPartSize = objectSize;
            long maxPartSize = MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE;

            if (eTagParts > 1) {
                minPartSize = (long) Math.ceil((float)objectSize/eTagParts);
                maxPartSize = (long) Math.floor((float)objectSize/(eTagParts - 1));
            } 

            // Detect using standard MB and the power of 2
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = MirrorConstants.MB;
                while (computedPartSize < minPartSize) {
                    computedPartSize *= 2;
                }
            }

            // Detect using MiB notation and the power of 2
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 1 * MirrorConstants.MiB;
                while (computedPartSize < minPartSize) {
                    computedPartSize *= 2;
                }
            }

            // Detect other special cases like s3n and Aspera
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 1 * MirrorConstants.MB;
                for (int i = 0; i < MirrorOptions.SPECIAL_PART_SIZES.length; i++) {
                    if (computedPartSize >= MirrorOptions.SPECIAL_PART_SIZES[i]) {
                        break;
                    } else {
                        computedPartSize = MirrorOptions.SPECIAL_PART_SIZES[i];
                    }
                }
            }

            // Detect if using 100MB increments
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 100 * MirrorConstants.MB;
                while (computedPartSize < minPartSize) {
                    computedPartSize += 100 * MirrorConstants.MB;
                }
            }

            // Detect if using 25MB increments up to 1GB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 25 * MirrorConstants.MB;
                while (computedPartSize < minPartSize || computedPartSize < 1 * MirrorConstants.GB) {
                    computedPartSize += 25 * MirrorConstants.MB;
                }
            }

            // Detect if using 10MB increments up to 1GB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 10 * MirrorConstants.MB;
                while (computedPartSize < minPartSize || computedPartSize < 1 * MirrorConstants.GB) {
                    computedPartSize += 10 * MirrorConstants.MB;
                }
            }

            // Detect if using 5MB increments up to 1GB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 5 * MirrorConstants.MB;
                while (computedPartSize < minPartSize || computedPartSize < 1 * MirrorConstants.GB) {
                    computedPartSize += 5 * MirrorConstants.MB;
                }
            }

            // Detect if using 1MB increments up to 100MB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 1 * MirrorConstants.MB;
                while (computedPartSize < minPartSize || computedPartSize < 100 * MirrorConstants.MB) {
                    computedPartSize += 1 * MirrorConstants.MB;
                }
            }

            partSize = computedPartSize;

            if (computedPartSize > maxPartSize) {
                if (options.isVerbose()) {
                    log.info("Could not automatically determine part size for " + summary.getKey() + ", reverting to " + optionsUploadPartSize/MB + "MB" );
                }
                partSize = optionsUploadPartSize;
            }

            if (computedPartSize < MirrorOptions.MINIMUM_PART_SIZE) {
                if (options.isVerbose()) {
                    log.info("Part size of " + computedPartSize/MB + "MB for " + summary.getKey() + " is greater than AWS minimum of " + MirrorOptions.MINIMUM_PART_SIZE/MB + "MB");
                }
                partSize = MirrorOptions.MINIMUM_PART_SIZE;
            }

        } else {
            if (options.isVerbose()) {
                log.info("Using cli override part size of " + optionsUploadPartSize/MB + "MB for " + summary.getKey());
            }
            partSize = optionsUploadPartSize;
        }

        long bytePosition = 0;

        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            String infoMessage = "copying : " + bytePosition + " to " + lastByte;
            if (options.isVerbose()) {
                log.info(infoMessage);
            }
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
                    if (options.isVerbose()) log.info("try :" + tries);
                    context.getStats().s3copyCount.incrementAndGet();
                    CopyPartResult copyPartResult = client.copyPart(copyRequest);
                    copyResponses.add(copyPartResult);
                    if (options.isVerbose()) log.info("completed " + infoMessage);
                    break;
                } catch (Exception e) {
                    if (tries == maxPartRetries) {
                        client.abortMultipartUpload(new AbortMultipartUploadRequest(
                                targetBucketName, keydest, initResult.getUploadId()));
                        log.error("Exception while doing multipart copy", e);
                        return false;
                    }
                }
            }
            bytePosition += partSize;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        client.completeMultipartUpload(completeRequest);
        if(options.isVerbose()) {
            log.info("completed multipart request for : " + summary.getKey());
        }
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
