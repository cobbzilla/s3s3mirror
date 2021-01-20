package org.cobbzilla.s3s3mirror.comparisonstrategies;

import org.cobbzilla.s3s3mirror.store.FileSummary;

public interface ComparisonStrategy {
    boolean sourceDifferent(FileSummary source, FileSummary destination);
}
