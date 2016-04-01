package org.cobbzilla.s3s3mirror.store.local;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.FileStoreFactory;
import org.cobbzilla.s3s3mirror.MirrorOptions;
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

        if (bucket.equals(MirrorOptions.READ_FILES_FROM_STDIN)) return new File(key);

        String slash = FileStoreFactory.findSlash(bucket, key);
        if (slash == null) slash = File.separator;

        if (FileStoreFactory.isLocalPath(bucket)) {

            bucket = stripTrailingSlash(bucket);
            if (key == null) return new File(bucket);

            key = stripLeadingSlash(key);

            final File file = new File(bucket + slash + key);
            log.info("getFile("+bucket+", "+key+") returning: "+file.getAbsolutePath());
            return file;

        } else {
            throw new IllegalArgumentException("Don't understand this as a directory: "+bucket);
        }
    }

    private static String stripLeadingSlash(String s) {
        if (s.startsWith("/") || s.startsWith("\\")) s = s.substring(1);
        return s;
    }

    private static String stripTrailingSlash(String s) {
        if (s.endsWith("/") || s.endsWith("\\")) s = s.substring(0, s.length()-1);
        return s;
    }

}
