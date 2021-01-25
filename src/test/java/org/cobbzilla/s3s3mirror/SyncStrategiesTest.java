package org.cobbzilla.s3s3mirror;

import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.EtagComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.SizeAndLastModifiedComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.SizeAndSha256ComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.SizeOnlyComparisonStrategy;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SyncStrategiesTest {
    private final EtagComparisonStrategy etagComparisonStrategy = new EtagComparisonStrategy();
    private final SizeOnlyComparisonStrategy sizeOnlyComparisonStrategy = new SizeOnlyComparisonStrategy();
    private final SizeAndLastModifiedComparisonStrategy sizeAndLastModifiedComparisonStrategy = new SizeAndLastModifiedComparisonStrategy();
    private final SizeAndSha256ComparisonStrategy sizeAndSha256ComparisonStrategy = new SizeAndSha256ComparisonStrategy(ignored -> "");

    private static final String CONTENT_IDENTIFIER_A = "CONTENT_IDENTIFIER_A";
    private static final String CONTENT_IDENTIFIER_B = "CONTENT_IDENTIFIER_B";
    private static final long SIZE_A = 0;
    private static final long SIZE_B = 1;
    private static final LocalDateTime TIME_EARLY = LocalDateTime.of(2020, 1, 1, 0, 0);
    private static final LocalDateTime TIME_LATER = TIME_EARLY.plusDays(1);


    @Test
    public void testEtaStrategyEtagAndSizeMatch() {
        FileSummary source = createTestFileSummaryWithEtag(SIZE_A, CONTENT_IDENTIFIER_A);
        FileSummary destination = createTestFileSummaryWithEtag(SIZE_A, CONTENT_IDENTIFIER_A);

        assertFalse(etagComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testEtagStrategySizeMatchEtagDoesNot() {
        FileSummary source = createTestFileSummaryWithEtag(SIZE_A, CONTENT_IDENTIFIER_A);
        FileSummary destination = createTestFileSummaryWithEtag(SIZE_A, CONTENT_IDENTIFIER_B);

        assertTrue(etagComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testEtagStrategyEtagMatchSizeDoesNot() {
        FileSummary source = createTestFileSummaryWithEtag(SIZE_A, CONTENT_IDENTIFIER_A);
        FileSummary destination = createTestFileSummaryWithEtag(SIZE_B, CONTENT_IDENTIFIER_A);

        assertTrue(etagComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeStrategySizeMatches() {
        FileSummary source = createTestFileSummary(SIZE_A);
        FileSummary destination = createTestFileSummary(SIZE_A);

        assertFalse(sizeOnlyComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeStrategySizeDoesNotMatches() {
        FileSummary source = createTestFileSummary(SIZE_A);
        FileSummary destination = createTestFileSummary(SIZE_B);

        assertTrue(sizeOnlyComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategySizeAndLastModifiedMatch() {
        FileSummary source = createTestFileSummary(SIZE_A, TIME_EARLY);
        FileSummary destination = createTestFileSummary(SIZE_A, TIME_EARLY);

        assertFalse(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategyLastModifiedMatchSizeDoesNot() {
        FileSummary source = createTestFileSummary(SIZE_A, TIME_EARLY);
        FileSummary destination = createTestFileSummary(SIZE_B, TIME_EARLY);

        assertTrue(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategySizeMatchDestinationAfterSource() {
        FileSummary source = createTestFileSummary(SIZE_A, TIME_EARLY);
        FileSummary destination = createTestFileSummary(SIZE_A, TIME_LATER);

        assertFalse(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndLastModifiedStrategySizeMatchSourceAfterDestination() {
        FileSummary source = createTestFileSummary(SIZE_A, TIME_LATER);
        FileSummary destination = createTestFileSummary(SIZE_A, TIME_EARLY);

        assertTrue(sizeAndLastModifiedComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndSha256StrategySizeAndShaMatch() {
        FileSummary source = createTestFileSummaryWithSha256(SIZE_A, CONTENT_IDENTIFIER_A);
        FileSummary destination = createTestFileSummaryWithSha256(SIZE_A, CONTENT_IDENTIFIER_A);

        assertFalse(sizeAndSha256ComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndSha256StrategyShaMatchSizeDoesNot() {
        FileSummary source = createTestFileSummaryWithSha256(SIZE_A, CONTENT_IDENTIFIER_A);
        FileSummary destination = createTestFileSummaryWithSha256(SIZE_B, CONTENT_IDENTIFIER_A);

        assertTrue(sizeAndSha256ComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndSha256StrategySizeMatchShaDoesNot() {
        FileSummary source = createTestFileSummaryWithSha256(SIZE_A, CONTENT_IDENTIFIER_A);
        FileSummary destination = createTestFileSummaryWithSha256(SIZE_A, CONTENT_IDENTIFIER_B);

        assertTrue(sizeAndSha256ComparisonStrategy.sourceDifferent(source, destination));
    }

    @Test
    public void testSizeAndSha256StrategyCallsOutForShaWhenNotThere() {
        AtomicBoolean flag = new AtomicBoolean(false);
        FileSummary source = createTestFileSummaryWithSha256(SIZE_A, Sha256.CHECK_OBJECT_METADATA);
        FileSummary destination = createTestFileSummaryWithSha256(SIZE_A, CONTENT_IDENTIFIER_A);

        ComparisonStrategy strategy = new SizeAndSha256ComparisonStrategy(ignored -> {
            flag.set(true);
            return "";
        });

        strategy.sourceDifferent(source, destination);

        assertTrue(flag.get());
    }

    private FileSummary createTestFileSummary(long size) {
        return createTestFileSummaryWithEtag(size, randomString());
    }

    private FileSummary createTestFileSummaryWithEtag(long size, String etag) {
        return createTestFileSummary(size, etag, LocalDateTime.now(), randomString());
    }

    private FileSummary createTestFileSummary(long size, LocalDateTime lastModifiedDate) {
        return createTestFileSummary(size, randomString(), lastModifiedDate, randomString());
    }

    private FileSummary createTestFileSummaryWithSha256(long size, String sha256) {
        return createTestFileSummary(size, randomString(), LocalDateTime.now(), sha256);
    }

    private FileSummary createTestFileSummary(long size, String etag, LocalDateTime lastModified, String sha256) {
        FileSummary summary = new FileSummary();

        summary.setETag(etag);
        summary.setSize(size);
        summary.setLastModified(lastModified.toEpochSecond(ZoneOffset.UTC));
        summary.setSha256(sha256);

        return summary;
    }

    private String randomString() {
        return Integer.toString(new Random().nextInt(1000));
    }
}
