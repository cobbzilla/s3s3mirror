package org.cobbzilla.s3s3mirror.store.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import org.cobbzilla.s3s3mirror.MirrorOptions;

import java.util.concurrent.atomic.AtomicReference;

public class S3ClientService {

    private static final AtomicReference<AmazonS3Client> s3client = new AtomicReference<AmazonS3Client>(null);

    public static AmazonS3Client getS3Client(MirrorOptions options) {
        synchronized (s3client) {
            if (s3client.get() == null) s3client.set(initS3Client(options));
        }
        return s3client.get();
    }

    public static AmazonS3Client initS3Client(MirrorOptions options) {

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol((options.isSsl() ? Protocol.HTTPS : Protocol.HTTP))
                .withMaxConnections(options.getMaxConnections());

        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }

        final AmazonS3Client client = new AmazonS3Client(options, clientConfiguration);
        if (options.hasEndpoint()) client.setEndpoint(options.getEndpoint());

        return client;
    }

}
