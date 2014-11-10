package org.cobbzilla.s3s3mirror.store.local;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileListing;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.ListRequest;

import java.io.File;

@Slf4j
public class LocalFileStore implements FileStore {

    public static final LocalFileStore instance = new LocalFileStore();

    public static File getFileAndCreateParent(String bucket, String key) {
        // if key starts with bucket, strip it from the front
        if (key.startsWith(bucket)) key = key.substring(bucket.length());

        // the "bucket" is merely the base directory name
        final File destFile = new File(bucket + File.separator + key);

        final File dir = destFile.getParentFile();
        if (!dir.exists() && !dir.mkdirs()) log.warn("Error creating parent directory: " + dir.getAbsolutePath());
        return destFile;
    }

    @Override public FileListing listObjects(ListRequest request, MirrorStats stats) {
        return new LocalFileListing(request);
    }

    @Override public FileListing listNextBatch(FileListing listing, MirrorStats stats) { return listing; }

    public static File getFile(String bucket) {
        return getFile(bucket, null);
    }

    public static File getFile(String bucket, String key) {
        if (bucket.startsWith(File.separator)) {
            return key == null ? new File(bucket) : new File(bucket + File.separator + key);
        } else {
            throw new IllegalArgumentException("Don't understand this as a directory: "+bucket);
        }
    }

}
