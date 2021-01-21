package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.cobbzilla.s3s3mirror.stats.MirrorStats;

public class BucketMirrorMaker {

    public MirrorStats makeCopy(MirrorOptions options) {
        options.initDerivedFields();
        options.setAwsCredentialProviders(new DefaultAWSCredentialsProviderChain());

        MirrorContext context = new MirrorContext(options);
        MirrorMaster mirrorMaster = new MirrorMaster(context);

        return mirrorMaster.mirror();
    }
}
