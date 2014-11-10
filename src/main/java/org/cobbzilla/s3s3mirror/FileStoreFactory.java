package org.cobbzilla.s3s3mirror;

import org.cobbzilla.s3s3mirror.store.s3.master.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class FileStoreFactory {

    public static KeyMaster buildCopyMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        final MirrorOptions options = context.getOptions();
        final String destination = options.getDestination();
        final String source = options.getSource();
        if (isLocalPath(destination)) {
            if (isLocalPath(source)) {
                throw new IllegalArgumentException("When both sides are local, wouldn't you prefer `rsync`, or even `cp -R` ?");
            }
            // copy S3->local
            return new S3ToLocalCopyMaster(context, workQueue, executor);

        } else if (isLocalPath(source)) {
            // copy local->S3
            return new LocalToS3CopyMaster(context, workQueue, executor);

        } else {
            // regular S3->S3 copy
            return new S3CopyMaster(context, workQueue, executor);
        }
    }

    public static boolean isLocalPath(String path) { return path.startsWith(".") || path.startsWith("/"); }

    // factory method for DeleteMaster
    public static KeyMaster buildDeleteMaster(MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executor) {
        final MirrorOptions options = context.getOptions();
        final String destination = options.getDestination();
        final String source = options.getSource();
        if (isLocalPath(destination)) {
            if (isLocalPath(source)) {
                throw new IllegalArgumentException("When both sides are local, wouldn't you prefer `rsync`?");
            }
            // deleting from local if not found in S3
            return new S3ToLocalDeleteMaster(context, workQueue, executor);

        } else if (isLocalPath(source)) {
            // deleting from S3 if not found in local
            return new LocalToS3DeleteMaster(context, workQueue, executor);

        } else {
            // regular S3->S3 delete behavior (delete from destination bucket if not found in source bucket)
            return new S3DeleteMaster(context, workQueue, executor);
        }
    }

}
