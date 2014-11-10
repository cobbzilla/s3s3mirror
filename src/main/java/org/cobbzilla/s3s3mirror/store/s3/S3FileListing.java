package org.cobbzilla.s3s3mirror.store.s3;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.cobbzilla.s3s3mirror.store.FileListing;
import org.cobbzilla.s3s3mirror.store.FileSummary;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@AllArgsConstructor
public class S3FileListing implements FileListing {

    @Getter @Setter private ObjectListing listing;

    @Override public List<FileSummary> getFileSummaries() {

        final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        final List<FileSummary> summaries = new ArrayList<FileSummary>(objectSummaries.size());

        for (S3ObjectSummary s : objectSummaries) summaries.add(buildSummary(s));

        return summaries;
    }

    @Override public boolean hasMore() { return listing.isTruncated(); }

    public static FileSummary buildSummary(S3ObjectSummary s) {
        final FileSummary summary = new FileSummary().setKey(s.getKey()).setSize(s.getSize()).setETag(s.getETag());
        final Date mtime = s.getLastModified();
        if (mtime != null) summary.setLastModified(mtime.getTime());
        return summary;
    }

    public static FileSummary buildSummary(String key, ObjectMetadata metadata) {
        if (metadata == null) return null; // key not found
        // warning: metadata.getContentLength returns 0 if it cannot be determined.
        return new FileSummary()
                .setKey(key)
                .setSize(metadata.getContentLength())
                .setLastModified(metadata.getLastModified().getTime())
                .setETag(metadata.getETag());

    }
}
