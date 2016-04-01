package org.cobbzilla.s3s3mirror.store.local;

import com.amazonaws.util.Md5Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.Sha256;
import org.cobbzilla.s3s3mirror.store.FileListing;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.ListRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class LocalFileListing implements FileListing {

    private ListRequest request;
    private Iterator<File> iterator;

    public LocalFileListing(ListRequest request) {
        this.request = request;
        if (request.getBucket().equals(MirrorOptions.READ_FILES_FROM_STDIN)) {
            iterator = new StdinFileIterator();
        } else {
            final File base = LocalFileStore.getFile(request.getBucket());
            if (!base.isDirectory()) throw new IllegalArgumentException("not a directory: " + base.getAbsolutePath());
            iterator = new LocalFileIterator(base, request.getPrefix());
        }
    }

    @Override public List<FileSummary> getFileSummaries() {

        final List<FileSummary> summaries = new ArrayList<FileSummary>(request.getFetchSize());

        for (int i=0; i<request.getFetchSize() && iterator.hasNext(); i++) {
            summaries.add(buildSummary(iterator.next(), request.getBucket()));
        }

        return summaries;
    }

    @Override public boolean hasMore() { return iterator.hasNext(); }

    public static FileSummary buildSummary(File file, String bucket) {
        if (!file.exists()) return null;

        final boolean isStdin = bucket.equals(MirrorOptions.READ_FILES_FROM_STDIN);

        String path;
        if (isStdin) {
            path = file.getPath();
        } else {
            path = file.getAbsolutePath();
            if (path.startsWith(bucket)) {
                path = path.substring(bucket.length());
                if (path.length() > 0 && path.startsWith("/")) path = path.substring(1);
            }
        }

        String linkTarget = null;
        try {
            if (FileUtils.isSymlink(file)) {
                linkTarget = Files.readSymbolicLink(file.toPath()).toString();
            }
        } catch (IOException e) {
            log.warn("buildSummary: error handling symlink (assuming regular file): "+e);
        }

        return new FileSummary()
                .setKey(path)
                .setETag(md5(file))
                .setSha256(Sha256.hash(file))
                .setLastModified(file.lastModified())
                .setSize(file.length())
                .setLinkTarget(linkTarget);
    }

    // Use the existing Md5Utils class in the AWS SDK
    protected static String md5(File file) {
      try {
        return Md5Utils.md5AsBase64(file);
      } catch(Exception e) {
        return null;
      }
    }
}
