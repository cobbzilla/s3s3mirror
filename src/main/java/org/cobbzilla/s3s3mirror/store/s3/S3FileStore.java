package org.cobbzilla.s3s3mirror.store.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileListing;
import org.cobbzilla.s3s3mirror.store.FileStore;
import org.cobbzilla.s3s3mirror.store.ListRequest;

@Slf4j
public class S3FileStore implements FileStore {

    @Getter @Setter private MirrorOptions options;
    @Getter private AmazonS3Client s3client;

    public S3FileStore (MirrorOptions options) {
        this.options = options;
        this.s3client = S3ClientService.getS3Client(options);
    }

    @Override
    public FileListing listObjects(ListRequest request, MirrorStats stats) {
        stats.s3getCount.incrementAndGet();
        final ListObjectsRequest list = new ListObjectsRequest(request.getBucket(), request.getPrefix(), null, null, request.getFetchSize());
        return new S3FileListing(s3client.listObjects(list));
    }

    @Override
    public FileListing listNextBatch(FileListing listing, MirrorStats stats) {
        stats.s3getCount.incrementAndGet();
        return new S3FileListing(s3client.listNextBatchOfObjects(((S3FileListing)listing).getListing()));
    }

    public static ObjectMetadata getObjectMetadata(String bucket, String key, MirrorContext context, AmazonS3Client s3Client) throws Exception {
        log.info("getObjectMetadata("+bucket+","+key+") starting....");
        MirrorOptions options = context.getOptions();
        Exception ex = null;
        for (int tries=0; tries<options.getMaxRetries(); tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                return s3Client.getObjectMetadata(bucket, key);

            } catch (AmazonS3Exception e) {
                ex = e;
                if (e.getStatusCode() == 404 || e.getStatusCode() == 403) return null;
                log.warn("Unrecognized Amazon exception: "+e);

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        log.error("getObjectMetadata(" + key + ") failed (try #" + tries + "), giving up");
                        break;
                    } else {
                        log.warn("getObjectMetadata(" + key + ") failed (try #" + tries + "), retrying...");
                    }
                }
            }
        }
        throw ex;
    }
}
