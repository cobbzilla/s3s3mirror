package org.cobbzilla.s3s3mirror.comparisonstrategies;

import org.cobbzilla.s3s3mirror.store.FileSummary;

public class SizeAndSha256ComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {
        return super.sourceDifferent(source, destination) || shasAreDifferent(source, destination);
    }

    private boolean shasAreDifferent(FileSummary source, FileSummary destination) {
        // todo get sha from object metadata

        // todo we are now missing verbose logging here.
        if (source.getSha256() == null || destination.getSha256() == null) {
            return true;
        }
        return !source.getSha256().equals(destination.getSha256());
    }
}
