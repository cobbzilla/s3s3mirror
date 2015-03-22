package org.cobbzilla.s3s3mirror.store.local;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.cobbzilla.s3s3mirror.store.FileListing;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.ListRequest;

import com.amazonaws.util.Md5Utils;

public class LocalFileListing implements FileListing {

    private ListRequest request;
    private LocalFileIterator iterator;

    public LocalFileListing(ListRequest request) {
        this.request = request;
        final File base = LocalFileStore.getFile(request.getBucket());
        if (!base.isDirectory()) throw new IllegalArgumentException("not a directory: "+base.getAbsolutePath());
        iterator = new LocalFileIterator(base, request.getPrefix());
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

        String path = file.getAbsolutePath();
        if (path.startsWith(bucket)) {
            path = path.substring(bucket.length());
            if (path.length() > 0 && path.startsWith("/")) path = path.substring(1);
        }
        
        return new FileSummary()               
                .setKey(path)
                .setETag(md5(file))
                .setLastModified(file.lastModified())
                .setSize(file.length());
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
