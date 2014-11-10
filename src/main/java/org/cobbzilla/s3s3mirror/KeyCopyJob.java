package org.cobbzilla.s3s3mirror;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.store.FileSummary;

@Slf4j
public abstract class KeyCopyJob implements KeyJob {

    protected final MirrorContext context;
    protected final FileSummary summary;
    protected final Object notifyLock;

    @Getter protected String keyDestination;

    protected KeyCopyJob(MirrorContext context, FileSummary summary, Object notifyLock) {
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;

        keyDestination = summary.getKey();
        final MirrorOptions options = context.getOptions();
        if (options.hasDestPrefix()) {
            keyDestination = keyDestination.substring(options.getPrefixLength());
            keyDestination = options.getDestPrefix() + keyDestination;
        }
    }

    protected abstract FileSummary getMetadata(String bucket, String key) throws Exception;
    protected abstract boolean copyFile() throws Exception;

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) return;

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + getKeyDestination());
            } else {
                if (tryCopy()) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("error copying key: " + key + ": " + e);
            if (!options.isDryRun()) context.getStats().copyErrors.incrementAndGet();

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("done with " + key);
        }
    }

    private boolean tryCopy() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        final String keydest = getKeyDestination();

        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);

            try {
                return copyFile();
            } catch (Exception e) {
                log.error("unexpected exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: " + key);
                return false;
            }
        }
        return false;
    }

    private boolean shouldTransfer() throws Exception {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Long lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (lastModified < options.getMaxAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+"), not copying");
                    return false;
                }
            }
        }

        final FileSummary destination = getMetadata(options.getDestinationBucket(), getKeyDestination());
        if (destination == null) return true;

        if (destination.getSize() != summary.getSize()) return true;

        final String destETag = destination.getETag();
        final String srcETag = summary.getETag();
        if (destETag != null && srcETag != null && !destETag.equals(srcETag)) return true;

        if (verbose) log.info("Destination file is same as source, not copying: "+ key);

        return false;
    }
}
