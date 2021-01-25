package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.ComparisonStrategyFactory;
import org.cobbzilla.s3s3mirror.comparisonstrategies.strategies.EtagComparisonStrategy;
import org.cobbzilla.s3s3mirror.comparisonstrategies.SyncStrategy;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;
import org.cobbzilla.s3s3mirror.store.s3.master.*;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public class FileStoreFactory {

    public static boolean isLocalPath(String path) {
        return path.startsWith(".\\") || path.startsWith("./")
                || path.startsWith("/") || path.startsWith("\\")
                || (path.indexOf(':') == 1 && path.charAt(2) == '\\')
                || path.equals(MirrorOptions.READ_FILES_FROM_STDIN);
    }

    public static KeyMaster buildCopyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        final MirrorOptions options = context.getOptions();
        final String destination = options.getDestination();
        final String source = options.getSource();
        final boolean verbose = options.isVerbose();

        ComparisonStrategy comparisonStrategy = ComparisonStrategyFactory.getStrategy(context.getOptions(), key -> {
            S3FileStore listSource = new S3FileStore(context.getOptions());
            AmazonS3Client s3client = listSource.getS3client();

            Map<String, String> userMetadata = S3FileStore.getObjectMetadata(context.getOptions().getSourceBucket(),
                                                                              key,
                                                                              context,
                                                                              s3client).getUserMetadata();
            return userMetadata == null ? "" : userMetadata.get(Sha256.S3S3_SHA256);
        });

        if (isLocalPath(destination)) {
            if (isLocalPath(source)) {
                throw new IllegalArgumentException("When both sides are local, wouldn't you prefer `rsync`, or even `cp -R` ?");
            }

            if (comparisonStrategy instanceof EtagComparisonStrategy) {
                throw new IllegalArgumentException("Etag comparison strategy isn't compatible with local files");
            }

            // copy S3->local
            if (verbose) log.info("CopyMaster will be S3ToLocalCopyMaster");
            return new S3ToLocalCopyMaster(context, workQueue, executor, comparisonStrategy);

        } else if (isLocalPath(source)) {
            // copy local->S3
            if (verbose) log.info("CopyMaster will be LocalToS3CopyMaster");

            if (comparisonStrategy instanceof EtagComparisonStrategy) {
                throw new IllegalArgumentException("Etag comparison strategy isn't compatible with local files");
            }

            return new LocalToS3CopyMaster(context, workQueue, executor, comparisonStrategy);

        } else {
            if (context.getOptions().getSyncStrategy() == SyncStrategy.AUTO) {
                comparisonStrategy = new EtagComparisonStrategy();
            }

            // regular S3->S3 copy
            if (verbose) log.info("CopyMaster will be S3CopyMaster");
            return new S3CopyMaster(context, workQueue, executor, comparisonStrategy);
        }
    }

    // factory method for DeleteMaster
    public static KeyMaster buildDeleteMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        final MirrorOptions options = context.getOptions();
        final String destination = options.getDestination();
        final String source = options.getSource();
        final boolean verbose = options.isVerbose();
        if (isLocalPath(destination)) {
            if (isLocalPath(source)) {
                throw new IllegalArgumentException("When both sides are local, wouldn't you prefer `rsync`?");
            }
            // deleting from local if not found in S3
            if (verbose) log.info("DeleteMaster will be S3ToLocalDeleteMaster");
            return new S3ToLocalDeleteMaster(context, workQueue, executor);

        } else if (isLocalPath(source)) {
            // deleting from S3 if not found in local
            if (verbose) log.info("DeleteMaster will be LocalToS3DeleteMaster");
            return new LocalToS3DeleteMaster(context, workQueue, executor);

        } else {
            // regular S3->S3 delete behavior (delete from destination bucket if not found in source bucket)
            if (verbose) log.info("DeleteMaster will be S3DeleteMaster");
            return new S3DeleteMaster(context, workQueue, executor);
        }
    }

    public static String findSlash(String... strings) {
        for (String s : strings) {
            if (s == null) continue;
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == '/') return "/";
                if (s.charAt(i) == '\\') return "\\";
            }
        }
        return null;
    }
}
