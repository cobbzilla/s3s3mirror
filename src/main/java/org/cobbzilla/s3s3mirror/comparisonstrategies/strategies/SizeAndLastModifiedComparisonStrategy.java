package org.cobbzilla.s3s3mirror.comparisonstrategies.strategies;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.store.FileSummary;

@Slf4j
public class SizeAndLastModifiedComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {
        if (super.sourceDifferent(source, destination)) {
            return true;
        }

        if (source.getLastModified() == null || destination.getLastModified() == null) {
            log.warn("Unable to get last modified datetime for source or destination for: " + source.getKey() + " requesting sync");
            return true;
        }
        return source.getLastModified() > destination.getLastModified();
    }
}
