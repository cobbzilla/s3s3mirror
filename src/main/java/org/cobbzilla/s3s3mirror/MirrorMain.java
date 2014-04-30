package org.cobbzilla.s3s3mirror;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Provides the "main" method. Responsible for parsing options and setting up the MirrorMaster to manage the copy.
 */
@Slf4j
public class MirrorMain {

    @Getter @Setter private String[] args;

    @Getter private final MirrorOptions options = new MirrorOptions();

    private final CmdLineParser parser = new CmdLineParser(options);

    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught Exception (thread "+t.getName()+"): "+e, e);
        }
    };

    @Getter private AmazonS3Client client;
    @Getter private MirrorContext context;
    @Getter private MirrorMaster master;

    public MirrorMain(String[] args) { this.args = args; }

    public static void main (String[] args) {
        MirrorMain main = new MirrorMain(args);
        main.run();
    }

    public void run() {
        init();
        master.mirror();
    }

    public void init() {
        if (client == null) {
            try {
                parseArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                parser.printUsage(System.err);
                System.exit(1);
            }

            client = getAmazonS3Client();
            context = new MirrorContext(options);
            master = new MirrorMaster(client, context);

            Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    protected AmazonS3Client getAmazonS3Client() {
        ClientConfiguration clientConfiguration = new ClientConfiguration().withProtocol(Protocol.HTTP)
                .withMaxConnections(options.getMaxConnections());
        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }
        AmazonS3Client client = null;
        if (options.hasAwsKeys()) {
            client = new AmazonS3Client(options, clientConfiguration);
        } else {
            client = new AmazonS3Client(new InstanceProfileCredentialsProvider(), clientConfiguration);
        }    
        if (options.hasEndpoint()) client.setEndpoint(options.getEndpoint());
        return client;
    }

    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        if (!options.hasAwsKeys()) {
            // try to load from ~/.s3cfg
            File s3cfg = new File(System.getProperty("user.home")+File.separator+".s3cfg");
            if (s3cfg.exists()) {
                @Cleanup BufferedReader reader = new BufferedReader(new FileReader(s3cfg));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().startsWith("access_key")) {
                        options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                    } else if (line.trim().startsWith("secret_key")) {
                        options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                    } else if (!options.getHasProxy() && line.trim().startsWith("proxy_host")) {
                        options.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
                    } else if (!options.getHasProxy() && line.trim().startsWith("proxy_port")){
                        options.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
                    }
                }
            }
        }
        /*
        if (!options.hasAwsKeys()) {
            throw new IllegalStateException("ENV vars not defined: " + MirrorOptions.AWS_ACCESS_KEY + " and/or " + MirrorOptions.AWS_SECRET_KEY);
        }*/
        options.initDerivedFields();
    }

}
