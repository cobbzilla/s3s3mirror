package org.cobbzilla.s3s3mirror.comparisonstrategies;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class EtagComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(S3ObjectSummary source, ObjectMetadata destination) {

        return super.sourceDifferent(source, destination) || !source.getETag().equals(destination.getETag());
    }
}
