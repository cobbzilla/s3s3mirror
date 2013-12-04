package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.AWSCredentials;
import lombok.Getter;
import lombok.Setter;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.concurrent.TimeUnit;

public class MirrorOptions implements AWSCredentials {

    public static final String S3_PROTOCOL_PREFIX = "s3://";

    public static final String AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID";
    public static final String AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY";
    @Getter @Setter private String aWSAccessKeyId = System.getenv().get(AWS_ACCESS_KEY);
    @Getter @Setter private String aWSSecretKey = System.getenv().get(AWS_SECRET_KEY);

    public boolean hasAwsKeys() { return aWSAccessKeyId != null && aWSSecretKey != null; }

    public static final String USAGE_DRY_RUN = "Do not actually do anything, but show what would be done";
    public static final String OPT_DRY_RUN = "-n";
    public static final String LONGOPT_DRY_RUN = "--dry-run";
    @Option(name=OPT_DRY_RUN, aliases=LONGOPT_DRY_RUN, usage=USAGE_DRY_RUN)
    @Getter @Setter private boolean dryRun = false;

    public static final String USAGE_VERBOSE = "Verbose output";
    public static final String OPT_VERBOSE = "-v";
    public static final String LONGOPT_VERBOSE = "--verbose";
    @Option(name=OPT_VERBOSE, aliases=LONGOPT_VERBOSE, usage=USAGE_VERBOSE)
    @Getter @Setter private boolean verbose = false;

    public static final String USAGE_PREFIX = "Only copy objects whose keys start with this prefix";
    public static final String OPT_PREFIX = "-p";
    public static final String LONGOPT_PREFIX = "--prefix";
    @Option(name=OPT_PREFIX, aliases=LONGOPT_PREFIX, usage=USAGE_PREFIX)
    @Getter @Setter private String prefix = null;

    public static final String USAGE_DEST_PREFIX = "Destination prefix (replacing the one specified in --prefix, if any)";
    public static final String OPT_DEST_PREFIX= "-d";
    public static final String LONGOPT_DEST_PREFIX = "--dest-prefix";
    @Option(name=OPT_DEST_PREFIX, aliases=LONGOPT_DEST_PREFIX, usage= USAGE_DEST_PREFIX)
    @Getter @Setter private String destPrefix = null;

    public boolean hasDestPrefix() { return destPrefix != null && destPrefix.trim().length() > 0; }

    public static final String USAGE_MAX_CONNECTIONS = "Maximum number of connections to S3 (default 100)";
    public static final String OPT_MAX_CONNECTIONS = "-m";
    public static final String LONGOPT_MAX_CONNECTIONS = "--max-connections";
    @Option(name=OPT_MAX_CONNECTIONS, aliases=LONGOPT_MAX_CONNECTIONS, usage=USAGE_MAX_CONNECTIONS)
    @Getter @Setter private int maxConnections = 100;

    public static final String USAGE_MAX_THREADS = "Maximum number of threads (default 100)";
    public static final String OPT_MAX_THREADS = "-t";
    public static final String LONGOPT_MAX_THREADS = "--max-threads";
    @Option(name=OPT_MAX_THREADS, aliases=LONGOPT_MAX_THREADS, usage=USAGE_MAX_THREADS)
    @Getter @Setter private int maxThreads = 100;

    public static final String USAGE_MAX_RETRIES = "Maximum number of retries for S3 requests (default 5)";
    public static final String OPT_MAX_RETRIES = "-r";
    public static final String LONGOPT_MAX_RETRIES = "--max-retries";
    @Option(name=OPT_MAX_RETRIES, aliases=LONGOPT_MAX_RETRIES, usage=USAGE_MAX_RETRIES)
    @Getter @Setter private int maxRetries = 5;

    public static final String USAGE_CTIME = "Only copy objects whose Last-Modified date is younger than this many days";
    public static final String OPT_CTIME = "-c";
    public static final String LONGOPT_CTIME = "--ctime";
    @Option(name=OPT_CTIME, aliases=LONGOPT_CTIME, usage=USAGE_CTIME)
    @Getter @Setter private Long ctime = null;
    public boolean hasCtime() { return ctime != null; }
    public long getCtimeMillis() { return TimeUnit.DAYS.toMillis(ctime); }

    @Getter private long nowTime = System.currentTimeMillis();

    @Argument(index=0, required=true, usage="source bucket") @Getter @Setter private String source;
    @Argument(index=1, required=true, usage="destination bucket") @Getter @Setter private String destination;

    public String getSourceBucket() { return scrubS3ProtocolPrefix(getSource()); }
    public String getDestinationBucket() { return scrubS3ProtocolPrefix(getDestination()); }

    protected String scrubS3ProtocolPrefix(String bucket) {
        bucket = bucket.trim();
        if (bucket.startsWith(S3_PROTOCOL_PREFIX)) {
            bucket = bucket.substring(S3_PROTOCOL_PREFIX.length());
        }
        return bucket;
    }

}
