package org.cobbzilla.s3s3mirror.comparisonstrategies;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.cobbzilla.s3s3mirror.MirrorOptions;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ComparisonStrategyFactory {
    public static ComparisonStrategy getStrategy(MirrorOptions mirrorOptions) {
        if (mirrorOptions.isSizeOnly()) {
            return new SizeOnlyComparisonStrategy();
        } else if (mirrorOptions.isSizeAndLastModified()) {
            return new SizeAndLastModifiedComparisonStrategy();
        } else {
            return new EtagComparisonStrategy();
        }
    }
}
