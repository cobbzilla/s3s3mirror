package org.cobbzilla.s3s3mirror;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.store.FileListing;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.ListRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KeyLister implements Runnable {

    private FileStore store;
    private MirrorContext context;
    private int maxQueueCapacity;

    private final List<FileSummary> summaries;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private FileListing listing;

    public boolean isDone () { return done.get(); }

    public KeyLister(FileStore store, MirrorContext context, int maxQueueCapacity, String bucket, String prefix) {
        this.store = store;
        this.context = context;
        this.maxQueueCapacity = maxQueueCapacity;

        final MirrorOptions options = context.getOptions();
        int fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<FileSummary>(10*fetchSize);

        final ListRequest request = new ListRequest(bucket, prefix, fetchSize);
        listing = getFirstBatch(store, request);
        synchronized (summaries) {
            final List<FileSummary> objectSummaries = listing.getFileSummaries();
            summaries.addAll(objectSummaries);
            context.getStats().objectsRead.addAndGet(objectSummaries.size());
            if (options.isVerbose()) log.info("added initial set of "+objectSummaries.size()+" keys");
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        int counter = 0;
        log.info("starting...");
        try {
            while (true) {
                while (getSize() < maxQueueCapacity) {
                    if (listing.hasMore()) {
                        listing = getNextBatch();
                        if (++counter % 100 == 0) context.getStats().logStats();
                        synchronized (summaries) {
                            final List<FileSummary> summaries = listing.getFileSummaries();
                            this.summaries.addAll(summaries);
                            context.getStats().objectsRead.addAndGet(summaries.size());
                            if (verbose) log.info("queued next set of "+summaries.size()+" keys (total now="+getSize()+")");
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
        } catch (Exception e) {
            log.error("Error in run loop, KeyLister thread now exiting: "+e);

        } finally {
            if (verbose) log.info("KeyLister run loop finished");
            done.set(true);
        }
    }

    private FileListing getFirstBatch(FileStore client, ListRequest request) {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        Exception lastException = null;
        for (int tries=0; tries<maxRetries; tries++) {
            try {
                final FileListing listing = client.listObjects(request, context.getStats());
                if (verbose) log.info("successfully got first batch of objects (on try #"+tries+")");
                return listing;

            } catch (Exception e) {
                lastException = e;
                log.warn("getFirstBatch: error listing (try #"+tries+"): "+e);
                if (Sleep.sleep(50)) {
                    log.info("getFirstBatch: interrupted while waiting for next try");
                    break;
                }
            }
        }
        throw new IllegalStateException("getFirstBatch: error listing: "+lastException, lastException);
    }

    private FileListing getNextBatch() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        for (int tries=0; tries<maxRetries; tries++) {
            try {
                FileListing next = store.listNextBatch(listing, context.getStats());
                if (verbose) log.info("successfully got next batch of objects (on try #"+tries+")");
                return next;

            } catch (Exception e) {
                log.error("exception listing objects (try #"+tries+"): "+e);
            }
            if (Sleep.sleep(50)) {
                log.info("getNextBatch: interrupted while waiting for next try");
                break;
            }
        }
        throw new IllegalStateException("Too many errors trying to list objects (maxRetries="+maxRetries+")");
    }

    private int getSize() {
        synchronized (summaries) {
            return summaries.size();
        }
    }

    public List<FileSummary> fetchNextBatch() {
        List<FileSummary> copy;
        synchronized (summaries) {
            copy = new ArrayList<FileSummary>(summaries);
            summaries.clear();
        }
        return copy;
    }
}