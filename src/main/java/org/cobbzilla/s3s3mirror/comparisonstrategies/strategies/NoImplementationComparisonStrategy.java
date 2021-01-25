package org.cobbzilla.s3s3mirror.comparisonstrategies.strategies;

import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;

public class NoImplementationComparisonStrategy implements ComparisonStrategy {
    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {
        throw new IllegalArgumentException("This type does not implement a comparison strategy");
    }
}
