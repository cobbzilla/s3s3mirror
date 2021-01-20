package org.cobbzilla.s3s3mirror.comparisonstrategies;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.EtagComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.SizeAndLastModifiedComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.SizeAndSha256ComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.SizeOnlyComparisonStrategy;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ComparisonStrategyFactory {
    public static ComparisonStrategy getStrategy(MirrorOptions mirrorOptions, S3Sha256Retriever s3MetadataAccessor) {
        switch(mirrorOptions.getSyncStrategy()) {
            case SIZE:
                return new SizeOnlyComparisonStrategy();
            case SIZE_ETAG:
                return new EtagComparisonStrategy();
            case SIZE_LAST_MODIFIED:
                return new SizeAndLastModifiedComparisonStrategy();
            default:
                return new SizeAndSha256ComparisonStrategy(s3MetadataAccessor);
        }
    }
}
