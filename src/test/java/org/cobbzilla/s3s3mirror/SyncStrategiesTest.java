package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.cobbzilla.s3s3mirror.comparisonstrategies.EtagComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.SizeAndLastModifiedComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.SizeOnlyComparisonStrategy;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class SyncStrategiesTest {
    private final EtagComparisonStrategy etagComparisonStrategy = new EtagComparisonStrategy();
    private final SizeOnlyComparisonStrategy sizeOnlyComparisonStrategy = new SizeOnlyComparisonStrategy();
    private final SizeAndLastModifiedComparisonStrategy sizeAndLastModifiedComparisonStrategy = new SizeAndLastModifiedComparisonStrategy();

    private static final String ETAG_A = "ETAG_A";
    private static final String ETAG_B = "ETAG_B";
    private static final long SIZE_A = 0;
    private static final long SIZE_B = 1;
    private static final LocalDateTime TIME_EARLY = LocalDateTime.of(2020, 1, 1, 0, 0);
    private static final LocalDateTime TIME_LATER = TIME_EARLY.plusDays(1);


    @Test
    public void testEtaStrategygEtagAndSizeMatch() {
        S3ObjectSummary source = createTestS3ObjectSummary(ETAG_A, SIZE_A);
        ObjectMetadata destination = createTestObjectMetadata(ETAG_A, SIZE_A);

        assertFalse(etagComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testEtagStrategySizeMatchEtagDoesNot() {
        S3ObjectSummary source = createTestS3ObjectSummary(ETAG_A, SIZE_A);
        ObjectMetadata destination = createTestObjectMetadata(ETAG_B, SIZE_A);

        assertTrue(etagComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testEtagStrategyEtagMatchSizeDoesNot() {
        S3ObjectSummary source = createTestS3ObjectSummary(ETAG_A, SIZE_A);
        ObjectMetadata destination = createTestObjectMetadata(ETAG_A, SIZE_B);

        assertTrue(etagComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeStrategySizeMatches() {
        S3ObjectSummary source = createTestS3ObjectSummary(SIZE_A);
        ObjectMetadata destination = createTestObjectMetadata(SIZE_A);

        assertFalse(sizeOnlyComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeStrategySizeDoesNotMatches() {
        S3ObjectSummary source = createTestS3ObjectSummary(SIZE_A);
        ObjectMetadata destination = createTestObjectMetadata(SIZE_B);

        assertTrue(sizeOnlyComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategySizeAndLastModifiedMatch() {
        S3ObjectSummary source = createTestS3ObjectSummary(SIZE_A, TIME_EARLY);
        ObjectMetadata destination = createTestObjectMetadata(SIZE_A, TIME_EARLY);

        assertFalse(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategyLastModifiedMatchSizeDoesNot() {
        S3ObjectSummary source = createTestS3ObjectSummary(SIZE_A, TIME_EARLY);
        ObjectMetadata destination = createTestObjectMetadata(SIZE_B, TIME_EARLY);

        assertTrue(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategySizeMatchDestinationAfterSource() {
        S3ObjectSummary source = createTestS3ObjectSummary(SIZE_A, TIME_EARLY);
        ObjectMetadata destination = createTestObjectMetadata(SIZE_A, TIME_LATER);

        assertFalse(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategySizeMatchSourceAfterDestination() {
        S3ObjectSummary source = createTestS3ObjectSummary(SIZE_A, TIME_LATER);
        ObjectMetadata destination = createTestObjectMetadata(SIZE_A, TIME_EARLY);

        assertTrue(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    private S3ObjectSummary createTestS3ObjectSummary(long size) {
        return createTestS3ObjectSummary(randomString(), size);
    }

    private S3ObjectSummary createTestS3ObjectSummary(String etag, long size) {
        return createTestS3ObjectSummary(etag, size, LocalDateTime.now());
    }

    private S3ObjectSummary createTestS3ObjectSummary(long size, LocalDateTime lastModifiedDate) {
        return createTestS3ObjectSummary(randomString(), size, lastModifiedDate);
    }

    private S3ObjectSummary createTestS3ObjectSummary(String etag, long size, LocalDateTime lastModified) {
        S3ObjectSummary summary = new S3ObjectSummary();

        summary.setETag(etag);
        summary.setSize(size);
        summary.setLastModified(Timestamp.valueOf(lastModified));

        return summary;
    }

    private ObjectMetadata createTestObjectMetadata(long size) {
        return createTestObjectMetadata(randomString(), size);
    }

    private ObjectMetadata createTestObjectMetadata(String etag, long size) {
        return createTestObjectMetadata(etag, size, LocalDateTime.now());
    }

    private ObjectMetadata createTestObjectMetadata(long size, LocalDateTime lastModified) {
        return createTestObjectMetadata(randomString(), size, lastModified);
    }

    private ObjectMetadata createTestObjectMetadata(String etag, long size, LocalDateTime lastModified) {
        ObjectMetadata metadata = mock(ObjectMetadata.class);

        doReturn(etag).when(metadata).getETag();
        doReturn(size).when(metadata).getContentLength();
        doReturn(Timestamp.valueOf(lastModified)).when(metadata).getLastModified();

        return metadata;
    }

    private String randomString() {
        return Integer.toString(new Random().nextInt(1000));
    }
}
