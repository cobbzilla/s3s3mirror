package org.cobbzilla.s3s3mirror.comparisonstrategies;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public interface ComparisonStrategy {
    boolean sourceDifferent(S3ObjectSummary source, ObjectMetadata destination);
}
