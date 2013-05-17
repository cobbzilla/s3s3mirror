package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KeyLister implements Runnable {

    private AmazonS3Client client;
    private MirrorOptions options;
    private int maxQueueCapacity;

    private final List<S3ObjectSummary> summaries;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private ObjectListing listing;

    public boolean isDone () { return done.get(); }

    public KeyLister(AmazonS3Client client, MirrorOptions options, int maxQueueCapacity) {
        this.client = client;
        this.options = options;
        this.maxQueueCapacity = maxQueueCapacity;

        int fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<S3ObjectSummary>(10* fetchSize);

        final ListObjectsRequest request = new ListObjectsRequest(options.getSourceBucket(), null, null, null, fetchSize);
        listing = client.listObjects(request);
        synchronized (summaries) {
            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
            summaries.addAll(objectSummaries);
            if (options.isVerbose()) log.info("added initial set of "+objectSummaries.size()+" keys");
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                while (getSize() < maxQueueCapacity/2) {
                    if (listing.isTruncated()) {
                        listing = client.listNextBatchOfObjects(listing);
                        synchronized (summaries) {
                            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
                            summaries.addAll(objectSummaries);
                            if (options.isVerbose()) log.info("queued next set of "+objectSummaries.size()+" keys (total now="+getSize()+")");
                        }

                    } else {
                        log.info("No more keys found in source bucket, exiting");
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    log.error("interrupted!");
                    return;
                }
            }
        } finally {
            done.set(true);
        }
    }

    private int getSize() {
        int size;
        synchronized (summaries) {
            size = summaries.size();
        }
        return size;
    }

    public List<S3ObjectSummary> getNextBatch() {
        List<S3ObjectSummary> copy;
        synchronized (summaries) {
            copy = new ArrayList<S3ObjectSummary>(summaries);
            summaries.clear();
        }
        return copy;
    }
}