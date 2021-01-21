package org.cobbzilla.s3s3mirror;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.stats.MirrorStats;

import java.util.concurrent.*;

/**
 * Sets up the task execution service and the KeyMasters to run the copy (and optionally delete) operations.
 */
@Slf4j
public class MirrorMaster {

    public static final String VERSION = System.getProperty("s3s3mirror.version");

    private final MirrorContext context;

    public MirrorMaster(MirrorContext context) { this.context = context; }

    public MirrorStats mirror() {
        ScheduledExecutorService scheduledExecutorService = null;
        log.info("version "+VERSION+" starting");

        final MirrorOptions options = context.getOptions();
        context.getStats().logFailedOperationInfo.set(options.isDetailedErrorLogging());

        if (options.isVerbose() && options.hasCtime()) log.info("will not copy anything older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+")");

        final int maxQueueCapacity = getMaxQueueCapacity(options);
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxQueueCapacity);
        final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.error("Error submitting job: "+r+", possible queue overflow");
            }
        };

        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(options.getMaxThreads(), options.getMaxThreads(), 1, TimeUnit.MINUTES, workQueue, rejectedExecutionHandler);

        if (context.getOptions().getStatusListener() != null) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            long rateInMillis = context.getOptions().getStatusListener().getUpdateInterval().toMillis();
            scheduledExecutorService.scheduleAtFixedRate(() -> context.getOptions().getStatusListener().provideStatus(context.getStats().copy()),
                                                         rateInMillis,
                                                         rateInMillis,
                                                         TimeUnit.MILLISECONDS);
        }

        final KeyMaster copyMaster = FileStoreFactory.buildCopyMaster(context, workQueue, executorService);
        KeyMaster deleteMaster = null;

        try {
            copyMaster.start();

            if (context.getOptions().isDeleteRemoved()) {
                deleteMaster = FileStoreFactory.buildDeleteMaster(context, workQueue, executorService);
                deleteMaster.start();
            }

            while (true) {
                if (copyMaster.isDone() && (deleteMaster == null || deleteMaster.isDone())) {
                    log.info("mirror: completed");
                    break;
                }
                if (Sleep.sleep(100)) {
                    context.getStats().completedFully.set(false);
                    return context.getStats();
                }
            }

        } catch (Exception e) {
            log.error("Unexpected exception in mirror: "+e, e);

        } finally {
            try { copyMaster.stop();   } catch (Exception e) { log.error("Error stopping copyMaster: "+e, e); }
            if (deleteMaster != null) {
                try { deleteMaster.stop(); } catch (Exception e) { log.error("Error stopping deleteMaster: "+e, e); }
            }
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdownNow();
            }
            // this will wait for currently executing tasks to finish, but there should be none by now
            executorService.shutdown();
        }

        return context.getStats();
    }

    public static int getMaxQueueCapacity(MirrorOptions options) {
        return 100 * options.getMaxThreads();
    }

}
