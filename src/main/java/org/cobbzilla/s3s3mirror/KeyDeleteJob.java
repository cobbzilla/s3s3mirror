package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import org.cobbzilla.s3s3mirror.stats.MirrorStats;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.NoImplementationComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;

public abstract class KeyDeleteJob extends KeyCopyJob {

    private String keysrc;

    public KeyDeleteJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock, new NoImplementationComparisonStrategy());

        final MirrorOptions options = context.getOptions();
        keysrc = summary.getKey(); // NOTE: summary.getKey is the key in the destination bucket
        if (options.hasPrefix()) {
            keysrc = keysrc.substring(options.getDestPrefixLength());
            keysrc = options.getPrefix() + keyDestination;
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
                getLog().info("Would have deleted "+key+" from destination because "+keysrc+" does not exist in source");
            } else {
                boolean deletedOK = false;
                for (int tries=0; tries<maxRetries; tries++) {
                    if (verbose) getLog().info("deleting (try #"+tries+"): "+key);
                    try {
                        deletedOK = deleteFile(options.getDestinationBucket(), key);
                        if (verbose) getLog().info("successfully deleted (on try #"+tries+"): "+key);
                        break;

                    } catch (Exception e) {
                        getLog().error("unexpected exception deleting (try #"+tries+") "+key+": "+e);
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        getLog().error("interrupted while waiting to retry key: "+key);
                        break;
                    }
                }
                if (deletedOK) {
                    stats.objectsDeleted.incrementAndGet();
                } else {
                    stats.addErroredDelete(this);
                }
            }

        } catch (Exception e) {
            getLog().error("error deleting key: "+key+": "+e);
            if (!options.isDryRun()) context.getStats().addErroredDelete(this);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) getLog().info("done with "+key);
        }
    }

    private boolean shouldDelete() {

        final MirrorOptions options = context.getOptions();

        // Does it exist in the source bucket
        try {
            final FileSummary metadata = getMetadata(options.getSourceBucket(), keysrc);
            if (metadata == null) {
                if (options.isVerbose()) getLog().info("Key not found in source bucket (will delete from destination): " + keysrc);
                return true;
            }
        } catch (Exception e) {
            getLog().warn("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
        }
        return false;
    }

}
