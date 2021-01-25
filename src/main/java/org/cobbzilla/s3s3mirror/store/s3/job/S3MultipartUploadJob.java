package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * For large files.
 * Uploads local->S3 if the object on S3 does not exist or is different than the local file.
 */
@Slf4j
public class S3MultipartUploadJob extends S3KeyUploadJob {

    @Override public Logger getLog() { return log; }

    public S3MultipartUploadJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        super(client, context, summary, notifyLock, comparisonStrategy);
    }

    @Override
    protected boolean copyFile() throws Exception {
        // adapted from http://docs.aws.amazon.com/AmazonS3/latest/dev/llJavaUploadFile.html

        // Create a list of UploadPartResponse objects. You get one of these for
        // each part upload.
        final List<PartETag> partETags = new ArrayList<PartETag>();
        final MirrorOptions options = context.getOptions();
        final String destBucket = options.getDestinationBucket();
        final String keydest = getKeyDestination();

        // Step 1: Initialize.
        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
                options.getDestinationBucket(), getKeyDestination());
        final InitiateMultipartUploadResult initResponse = s3client.initiateMultipartUpload(initRequest);
        options.apply(initRequest);

        final File file = LocalFileStore.getFile(options.getSourceBucket(), summary.getKey());
        final long contentLength = file.length();
        long partSize = options.getUploadPartSize();

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 5 MB. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create request to upload a part.
                final UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(destBucket).withKey(keydest)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);
                options.apply(uploadRequest);

                // Upload part and add response to our list.
                partETags.add(s3client.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            final CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                    destBucket, keydest, initResponse.getUploadId(), partETags);
            s3client.completeMultipartUpload(compRequest);
            return true;

        } catch (Exception e) {
            log.error("Error uploading: "+e);
            s3client.abortMultipartUpload(new AbortMultipartUploadRequest(destBucket, keydest, initResponse.getUploadId()));
        }
        return false;
    }
}
