package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class MirrorMaster {

    private final AmazonS3Client client;
    private final MirrorOptions options;
    private final Object notifyLock = new Object();

    public MirrorMaster(AmazonS3Client client, MirrorOptions options) {
        this.client = client;
        this.options = options;
    }

    public void mirror() {

        final boolean verbose = options.isVerbose();

        final int maxQueueCapacity = 10 * options.getMaxThreads();
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(maxQueueCapacity);
        final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.error("Error submitting job: "+r+", possible queue overflow");
            }
        };

        final ExecutorService executorService = new ThreadPoolExecutor(options.getMaxThreads(), options.getMaxThreads(), 1, TimeUnit.MINUTES, workQueue, rejectedExecutionHandler);
        final KeyLister lister = new KeyLister(client, options, maxQueueCapacity);
        executorService.submit(lister);

        List<S3ObjectSummary> summaries = lister.getNextBatch();
        if (verbose) log.info(summaries.size()+" keys found in first batch from source bucket -- processing...");

        int counter = 0;
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
                executorService.submit(new KeyJob(client, options, summary, notifyLock));
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
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("interrupted!");
                    return;
                }
            }
        }

    }
}
