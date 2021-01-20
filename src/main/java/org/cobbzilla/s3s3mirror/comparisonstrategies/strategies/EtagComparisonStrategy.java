package org.cobbzilla.s3s3mirror.comparisonstrategies.strategies;

import org.cobbzilla.s3s3mirror.store.FileSummary;

public class EtagComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {

        return super.sourceDifferent(source, destination) || !source.getETag().equals(destination.getETag());
    }
}
