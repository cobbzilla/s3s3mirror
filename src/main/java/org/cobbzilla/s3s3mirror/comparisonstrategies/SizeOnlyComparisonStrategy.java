package org.cobbzilla.s3s3mirror.comparisonstrategies;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class SizeOnlyComparisonStrategy implements ComparisonStrategy {
    @Override
    public boolean sourceDifferent(S3ObjectSummary source, ObjectMetadata destination) {
        return source.getSize() != destination.getContentLength();
    }
}
