package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;
import org.slf4j.Logger;

@Slf4j
public abstract class KeyCopyJob implements KeyJob {

    protected AmazonS3Client s3client;
    protected final MirrorContext context;
    protected final FileSummary summary;
    protected final Object notifyLock;
    protected final ComparisonStrategy comparisonStrategy;

    @Getter protected String keyDestination;

    protected KeyCopyJob(AmazonS3Client s3client, MirrorContext context, FileSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        this.s3client = s3client;
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;
        this.comparisonStrategy = comparisonStrategy;

        keyDestination = summary.getKey();
        final MirrorOptions options = context.getOptions();
        if (!options.hasDestPrefix()
                && options.hasPrefix()
                && keyDestination.startsWith(options.getPrefix())
                && keyDestination.length() > options.getPrefixLength()) {
            keyDestination = keyDestination.substring(options.getPrefixLength());

        } else if (options.hasDestPrefix()) {
            keyDestination = keyDestination.substring(options.getPrefixLength());
            keyDestination = options.getDestPrefix() + keyDestination;
        }
        // If the destination is not local, ensure any windows separators are changed to S3 separators
        if (!FileStoreFactory.isLocalPath(options.getDestination())) {
            keyDestination = keyDestination.replace("\\", "/");
        }
    }

    protected ObjectMetadata getObjectMetadata(String bucket, String key) throws Exception {
        return S3FileStore.getObjectMetadata(bucket, key, context, s3client);
    }

    protected abstract FileSummary getMetadata(String bucket, String key) throws Exception;
    protected abstract boolean copyFile() throws Exception;
    protected abstract Logger getLog();

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) return;

            if (options.isDryRun()) {
                getLog().info("Would have copied " + key + " to destination: " + getKeyDestination());
            } else {
                if (tryCopy()) {
                    if (options.isVerbose()) getLog().info("successfully copied "+key+" -> "+getKeyDestination());
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().addErroredCopy(this);
                }
            }
        } catch (Exception e) {
            getLog().error("error copying key: " + key + ": " + e);
            if (!options.isDryRun()) context.getStats().addErroredCopy(this);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) getLog().info("done with " + key);
        }
    }

    private boolean tryCopy() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        final String keydest = getKeyDestination();

        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) getLog().info("copying (try #" + tries + "): " + key + " to: " + keydest);

            try {
                return copyFile();
            } catch (Exception e) {
                getLog().error("unexpected exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                getLog().error("interrupted while waiting to retry key: " + key);
                return false;
            }
        }
        return false;
    }

    private boolean shouldTransfer() throws Exception {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasRegex() && !options.getRegexPattern().matcher(key).matches()) {
            if (verbose) getLog().info("shouldTransfer: Regex ("+options.getRegex()+") does not match key (), return false");
            return false;
        }

        if (options.hasCtime()) {
            final Long lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) getLog().info("shouldTransfer: No Last-Modified header for key: " + key);

            } else {
                if (lastModified < options.getMaxAge()) {
                    if (verbose) getLog().info("shouldTransfer: key " + key + " (lastmod=" + lastModified + ") is older than " + options.getCtime() + " (cutoff=" + options.getMaxAgeDate() + "), returning false");
                    return false;
                }
            }
        }

        final FileSummary destination = getMetadata(options.getDestinationBucket(), getKeyDestination());
        if (destination == null) {
            if (verbose) getLog().info("shouldTransfer: destination key ("+getKeyDestination()+") does not exist, returning true");
            return true;
        }

        return comparisonStrategy.sourceDifferent(summary, destination);
    }

    @Override
    public String getSource() {
        return summary.getKey();
    }

    @Override
    public String getDestination() {
        return keyDestination;
    }
}
