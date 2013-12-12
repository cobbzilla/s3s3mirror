package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;

/**
 * Manages the Starts a KeyLister and sends batches of keys to the ExecutorService for handling by KeyJobs
 */
@Slf4j
public class MirrorMaster {

    public static final String VERSION = System.getProperty("s3s3mirror.version");

    private final AmazonS3Client client;
    private final MirrorContext context;
    private final Object notifyLock = new Object();

    public MirrorMaster(AmazonS3Client client, MirrorContext context) {
        this.client = client;
        this.context = context;
    }

    public void mirror() {

        log.info("version "+VERSION+" starting");

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        final int maxQueueCapacity = 10 * options.getMaxThreads();
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxQueueCapacity);
        final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.error("Error submitting job: "+r+", possible queue overflow");
            }
        };

        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(options.getMaxThreads(), options.getMaxThreads(), 1, TimeUnit.MINUTES, workQueue, rejectedExecutionHandler);
        final KeyLister lister = new KeyLister(client, context, maxQueueCapacity);
        executorService.submit(lister);

        List<S3ObjectSummary> summaries = lister.getNextBatch();
        if (verbose) log.info(summaries.size()+" keys found in first batch from source bucket -- processing...");

        int counter = 0;
        try {
            while (true) {
                for (S3ObjectSummary summary : summaries) {
                    while (workQueue.size() >= maxQueueCapacity) {
                        try {
                            synchronized (notifyLock) {
                                notifyLock.wait(100);
                            }
                            Thread.sleep(50);

                        } catch (InterruptedException e) {
                            log.error("interrupted!");
                            return;
                        }
                    }
                    executorService.submit(new KeyJob(client, context, summary, notifyLock));
                    counter++;
                }

                summaries = lister.getNextBatch();
                if (summaries.size() > 0) {
                    if (verbose) log.info(summaries.size()+" more keys found in source bucket -- continuing (queue size="+workQueue.size()+", total processed="+counter+")...");

                } else if (lister.isDone()) {
                    if (verbose) log.info("No more keys found in source bucket -- ALL DONE");
                    return;

                } else {
                    if (verbose) log.info("Lister has no keys queued, but is not done, waiting and retrying");
                    if (sleep(100)) return;
                }
            }

        } catch (Exception e) {
            log.error("Unexpected exception in MirrorMaster: "+e, e);

        } finally {
            while (workQueue.size() > 0 || executorService.getActiveCount() > 0) {
                // wait for the queue to be empty
                if (sleep(100)) break;
            }
            // this will wait for currently executing tasks to finish
            executorService.shutdown();
        }
    }

    private boolean sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("interrupted!");
            return true;
        }
        return false;
    }
}
