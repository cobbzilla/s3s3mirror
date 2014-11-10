package org.cobbzilla.s3s3mirror;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.FileSummary;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class KeyMaster implements Runnable {

    public static final int STOP_TIMEOUT_SECONDS = 10;
    private static final long STOP_TIMEOUT = TimeUnit.SECONDS.toMillis(STOP_TIMEOUT_SECONDS);

    protected MirrorContext context;

    private AtomicBoolean done = new AtomicBoolean(false);
    public boolean isDone () { return done.get(); }

    private BlockingQueue<Runnable> workQueue;
    private ThreadPoolExecutor executor;
    protected final Object notifyLock = new Object();

    private Thread thread;

    public KeyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        this.context = context;
        this.workQueue = workQueue;
        this.executor = executor;
    }

    protected abstract FileStore getListSource();

    protected abstract String getPrefix(MirrorOptions options);
    protected abstract String getBucket(MirrorOptions options);

    protected abstract KeyJob getTask(FileSummary summary);

    public void start () {
        this.thread = new Thread(this);
        this.thread.start();
    }

    public void stop () {
        final String name = getClass().getSimpleName();
        final long start = System.currentTimeMillis();
        log.info("stopping "+ name +"...");
        try {
            if (isDone()) return;
            this.thread.interrupt();
            while (!isDone() && System.currentTimeMillis() - start < STOP_TIMEOUT) {
                if (Sleep.sleep(50)) return;
            }
        } finally {
            if (!isDone()) {
                try {
                    log.warn(name+" didn't stop within "+STOP_TIMEOUT_SECONDS+" after interrupting it, forcibly killing the thread...");
                    this.thread.stop();
                } catch (Exception e) {
                    log.error("Error calling Thread.stop on " + name + ": " + e, e);
                }
            }
            if (isDone()) log.info(name+" stopped");
        }
    }

    public void run() {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();

        final int maxQueueCapacity = MirrorMaster.getMaxQueueCapacity(options);

        int counter = 0;
        try {
            final KeyLister lister = new KeyLister(getListSource(), context, maxQueueCapacity, getBucket(options), getPrefix(options));
            executor.submit(lister);

            List<FileSummary> summaries = lister.fetchNextBatch();
            if (verbose) log.info(summaries.size()+" keys found in first batch from source bucket -- processing...");

            while (true) {
                for (FileSummary summary : summaries) {
                    while (workQueue.size() >= maxQueueCapacity) {
                        try {
                            synchronized (notifyLock) {
                                notifyLock.wait(50);
                            }
                            Thread.sleep(50);

                        } catch (InterruptedException e) {
                            log.error("interrupted!");
                            return;
                        }
                    }
                    executor.submit(getTask(summary));
                    counter++;
                }

                summaries = lister.fetchNextBatch();
                if (summaries.size() > 0) {
                    if (verbose) log.info(summaries.size()+" more keys found in source bucket -- continuing (queue size="+workQueue.size()+", total processed="+counter+")...");

                } else if (lister.isDone()) {
                    if (verbose) log.info("No more keys found in source bucket -- ALL DONE");
                    return;

                } else {
                    if (verbose) log.info("Lister has no keys queued, but is not done, waiting and retrying");
                    if (Sleep.sleep(50)) return;
                }
            }

        } catch (Exception e) {
            log.error("Unexpected exception in MirrorMaster ("+getClass().getName()+"): "+e, e);

        } finally {
            while (workQueue.size() > 0 || executor.getActiveCount() > 0) {
                // wait for the queue to be empty
                if (Sleep.sleep(100)) break;
            }
            done.set(true);
        }
    }
}
