package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Provides the "main" method. Responsible for parsing options, building the context,
 * setting up the shutdown hook, then running a MirrorMaster to do the copy.
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

    @Getter private MirrorContext context;
    @Getter private MirrorMaster master = null;

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
        if (master == null) {
            try {
                parseArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                parser.printUsage(System.err);
                System.exit(1);
            }

            context = new MirrorContext(options);
            master = new MirrorMaster(context);

            Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        if (!options.hasAwsKeys()) {
            // try to load from ~/.s3cfg
            try {
                @Cleanup BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.home") + File.separator + ".s3cfg"));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().startsWith("access_key")) {
                        options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                    } else if (line.trim().startsWith("secret_key")) {
                        options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                    } else if (!options.getHasProxy() && line.trim().startsWith("proxy_host")) {
                        options.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
                    } else if (!options.getHasProxy() && line.trim().startsWith("proxy_port")) {
                        options.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
                    }
                }
                options.setAwsCredentialProviders(new AWSCredentialsProviderChain(new AWSStaticCredentialsProvider(new BasicAWSCredentials(options.getAWSAccessKeyId(), options.getAWSSecretKey()))));
                log.info("Using s3cfg credentials");
            } catch (Exception e) {
                // likely file not found, so log at debug and continue to try default aws creds next
                log.debug("Could not load credentials from ~/.s3cfg", e);
            }
        }
        if (!options.hasAwsKeys()) {
            // try to load creds from env vars, system properties, profiles, or EC2 instance profile
            options.setAwsCredentialProviders(new DefaultAWSCredentialsProviderChain());
            if (options.getAwsCredentialProviders().getCredentials() == null)
                throw new IllegalStateException("ENV vars not defined: " + MirrorOptions.AWS_ACCESS_KEY + " and/or " + MirrorOptions.AWS_SECRET_KEY);
            log.info("Using aws default credential provider");
        }
        options.initDerivedFields();
    }

}
