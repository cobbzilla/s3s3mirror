package org.cobbzilla.s3s3mirror;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.store.FileSummary;

@Slf4j
public abstract class KeyDeleteJob extends KeyCopyJob {

    private String keysrc;

    public KeyDeleteJob(MirrorContext context, FileSummary summary, Object notifyLock) {
        super(context, summary, notifyLock);

        final MirrorOptions options = context.getOptions();
        keysrc = summary.getKey(); // NOTE: summary.getKey is the key in the destination bucket
        if (options.hasPrefix()) {
            keysrc = keysrc.substring(options.getDestPrefixLength());
            keysrc = options.getPrefix() + keysrc;
        }
    }

    @Override protected boolean copyFile() throws Exception { throw new IllegalStateException("copyFile not supported"); }

    protected abstract boolean deleteFile(String bucket, String key) throws Exception;

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldDelete()) return;

            if (options.isDryRun()) {
                log.info("Would have deleted "+key+" from destination because "+keysrc+" does not exist in source");
            } else {
                boolean deletedOK = false;
                for (int tries=0; tries<maxRetries; tries++) {
                    if (verbose) log.info("deleting (try #"+tries+"): "+key);
                    try {
                        deletedOK = deleteFile(options.getDestinationBucket(), key);
                        if (verbose) log.info("successfully deleted (on try #"+tries+"): "+key);
                        break;

                    } catch (Exception e) {
                        log.error("unexpected exception deleting (try #"+tries+") "+key+": "+e);
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        log.error("interrupted while waiting to retry key: "+key);
                        break;
                    }
                }
                if (deletedOK) {
                    stats.objectsDeleted.incrementAndGet();
                } else {
                    stats.deleteErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            log.error("error deleting key: "+key+": "+e);
            if (!options.isDryRun()) context.getStats().deleteErrors.incrementAndGet();

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("done with "+key);
        }
    }

    private boolean shouldDelete() {

        final MirrorOptions options = context.getOptions();

        // Does it exist in the source bucket
        try {
            final FileSummary metadata = getMetadata(options.getSourceBucket(), keysrc);
            if (metadata == null) {
                if (options.isVerbose()) log.info("Key not found in source bucket (will delete from destination): " + keysrc);
                return true;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
        }
        return false;
    }

}
