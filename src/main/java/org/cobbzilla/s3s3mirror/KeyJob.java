package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;

public abstract class KeyJob implements Runnable {

    protected final AmazonS3Client client;
    protected final MirrorContext context;
    protected final S3ObjectSummary summary;
    protected final Object notifyLock;

    public KeyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        this.client = client;
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;
    }

    public abstract Logger getLog();

    @Override public String toString() { return summary.getKey(); }

    protected ObjectMetadata getObjectMetadata(String bucket, String key, MirrorOptions options) throws Exception {
        Exception ex = null;
        for (int tries=0; tries<options.getMaxRetries(); tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                return client.getObjectMetadata(bucket, key);

            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) throw e;

            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        getLog().error("getObjectMetadata(" + key + ") failed (try #" + tries + "), giving up");
                        break;
                    } else {
                        getLog().warn("getObjectMetadata("+key+") failed (try #"+tries+"), retrying...");
                    }
                }
            }
        }
        throw ex;
    }

    protected AccessControlList getAccessControlList(MirrorOptions options, String key) throws Exception {
        Exception ex = null;

        for (int tries=0; tries<=options.getMaxRetries(); tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                return client.getObjectAcl(options.getSourceBucket(), key);

            } catch (Exception e) {
                ex = e;

                if (tries >= options.getMaxRetries()) {
                    // Annoyingly there can be two reasons for this to fail. It will fail if the IAM account
                    // permissions are wrong, but it will also fail if we are copying an item that we don't
                    // own ourselves. This may seem unusual, but it occurs when copying AWS Detailed Billing
                    // objects since although they live in your bucket, the object owner is AWS.
                    getLog().warn("Unable to obtain object ACL, copying item without ACL data.");
                    return new AccessControlList();
		}

                if (options.isVerbose()) {
                   if (tries >= options.getMaxRetries()) {
                        getLog().warn("getObjectAcl(" + key + ") failed (try #" + tries + "), giving up.");
			break;
                    } else {
                        getLog().warn("getObjectAcl("+key+") failed (try #"+tries+"), retrying...");
                    }
                }
            }
        }
        throw ex;
    }

    AccessControlList buildCrossAccountAcl(AccessControlList original) {
        AccessControlList result = new AccessControlList();
        for (Grant grant : original.getGrantsAsList()) {
            // Covers all 3 types: Everyone, Authenticate User, Log Delivery
            if (grant.getGrantee() instanceof GroupGrantee) {
                result.grantPermission(grant.getGrantee(), grant.getPermission());
            }
        }

        // Equal to the canned way: request.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
        result.grantPermission(new CanonicalGrantee(context.getOwner().getId()), Permission.FullControl);
        result.setOwner(context.getOwner());

        return result;
    }

}
