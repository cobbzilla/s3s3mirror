package org.cobbzilla.s3s3mirror.comparisonstrategies.strategies;

import lombok.SneakyThrows;
import org.cobbzilla.s3s3mirror.Sha256;
import org.cobbzilla.s3s3mirror.comparisonstrategies.S3Sha256Retriever;
import org.cobbzilla.s3s3mirror.store.FileSummary;

public class SizeAndSha256ComparisonStrategy extends SizeOnlyComparisonStrategy {
    private final S3Sha256Retriever s3ShaReceiver;
    public SizeAndSha256ComparisonStrategy(S3Sha256Retriever s3ShaRetriever) {
        this.s3ShaReceiver = s3ShaRetriever;
    }

    @Override
    public boolean sourceDifferent(FileSummary source, FileSummary destination) {
        return super.sourceDifferent(source, destination) || shasAreDifferent(source, destination);
    }

    @SneakyThrows
    private boolean shasAreDifferent(FileSummary source, FileSummary destination) {
        if (source.getSha256() == null || destination.getSha256() == null) {
            return true;
        }

        String sourceSha256 = source.getSha256();

        if (source.getSha256().equals(Sha256.CHECK_OBJECT_METADATA)) {
            sourceSha256 = s3ShaReceiver.getSha(source.getKey());
        }

        return !destination.getSha256().equals(sourceSha256);
    }
}
