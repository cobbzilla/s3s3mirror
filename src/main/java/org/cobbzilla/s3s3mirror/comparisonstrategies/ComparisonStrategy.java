package org.cobbzilla.s3s3mirror.comparisonstrategies;

import org.cobbzilla.s3s3mirror.store.FileSummary;

public interface ComparisonStrategy {
    // todo have some method of checking if strategy is compatible.

    boolean sourceDifferent(FileSummary source, FileSummary destination);
}
