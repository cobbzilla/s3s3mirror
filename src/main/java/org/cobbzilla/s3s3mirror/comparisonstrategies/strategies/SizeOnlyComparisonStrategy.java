package org.cobbzilla.s3s3mirror.comparisonstrategies.strategies;

import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;

public class SizeOnlyComparisonStrategy implements ComparisonStrategy {
    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {
        return  source.getSize() != destination.getSize();
    }
}
