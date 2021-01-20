package org.cobbzilla.s3s3mirror.comparisonstrategies;

import org.cobbzilla.s3s3mirror.store.FileSummary;

public class SizeAndLastModifiedComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {
        // todo handle null destinations.
        return super.sourceDifferent(source, destination) || source.getLastModified() < destination.getLastModified();
    }
}
