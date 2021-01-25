package org.cobbzilla.s3s3mirror;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.stats.StatusListener;
import org.cobbzilla.s3s3mirror.comparisonstrategies.SyncStrategy;
import org.joda.time.DateTime;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.util.Date;
import java.util.regex.Pattern;

import static org.cobbzilla.s3s3mirror.MirrorConstants.GB;

@Slf4j
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MirrorOptions implements AWSCredentials {

    public static final String S3_PROTOCOL_PREFIX = "s3://";
    public static final String sep = File.separator;
    public static final String READ_FILES_FROM_STDIN = ".-";

    public static final String AWS_ACCESS_KEY = "AWS_ACCESS_KEY_ID";
    public static final String AWS_SECRET_KEY = "AWS_SECRET_ACCESS_KEY";
    @Getter @Setter private String aWSAccessKeyId = System.getenv().get(AWS_ACCESS_KEY);
    @Getter @Setter private String aWSSecretKey = System.getenv().get(AWS_SECRET_KEY);

    public boolean hasAwsKeys() { return aWSAccessKeyId != null && aWSSecretKey != null; }

    @Getter @Setter private AWSCredentialsProviderChain awsCredentialProviders;

    @Getter @Setter private boolean detailedErrorLogging;
    @Getter @Setter private StatusListener statusListener;

    public static final String USAGE_DRY_RUN = "Do not actually do anything, but show what would be done";
    public static final String OPT_DRY_RUN = "-n";
    public static final String LONGOPT_DRY_RUN = "--dry-run";
    @Option(name=OPT_DRY_RUN, aliases=LONGOPT_DRY_RUN, usage=USAGE_DRY_RUN)
    @Builder.Default
    @Getter @Setter private boolean dryRun = false;

    public static final String USAGE_VERBOSE = "Verbose output";
    public static final String OPT_VERBOSE = "-v";
    public static final String LONGOPT_VERBOSE = "--verbose";
    @Option(name=OPT_VERBOSE, aliases=LONGOPT_VERBOSE, usage=USAGE_VERBOSE)
    @Builder.Default
    @Getter @Setter private boolean verbose = false;

    public static final String USAGE_SSL = "Use SSL for all S3 api operations";
    public static final String OPT_SSL = "-s";
    public static final String LONGOPT_SSL = "--ssl";
    @Option(name=OPT_SSL, aliases=LONGOPT_SSL, usage=USAGE_SSL)
    @Builder.Default
    @Getter @Setter private boolean ssl = false;

    public static final String USAGE_ENCRYPT = "Enable AWS managed server-side encryption";
    public static final String OPT_ENCRYPT = "-E";
    public static final String LONGOPT_ENCRYPT = "--server-side-encryption";
    @Option(name=OPT_ENCRYPT, aliases=LONGOPT_ENCRYPT, usage=USAGE_ENCRYPT)
    @Builder.Default
    @Getter @Setter private boolean encrypt = false;

    public static final String USAGE_STORAGE_CLASS = "The S3 StorageClass (default Standard)";
    public static final String OPT_STORAGE_CLASS = "-l";
    public static final String LONGOPT_STORAGE_CLASS = "--storage-class";
    @Option(name=OPT_STORAGE_CLASS, aliases=LONGOPT_STORAGE_CLASS, usage=USAGE_STORAGE_CLASS)
    @Builder.Default
    @Getter @Setter private S3StorageClass storageClass = S3StorageClass.Standard;

    public static final String USAGE_PREFIX = "Only copy objects whose keys start with this prefix";
    public static final String OPT_PREFIX = "-p";
    public static final String LONGOPT_PREFIX = "--prefix";
    @Option(name=OPT_PREFIX, aliases=LONGOPT_PREFIX, usage=USAGE_PREFIX)
    @Getter @Setter private String prefix = null;

    public boolean hasPrefix () { return prefix != null && prefix.length() > 0; }
    public int getPrefixLength () { return prefix == null ? 0 : prefix.length(); }

    public static final String USAGE_DEST_PREFIX = "Destination prefix (replacing the one specified in --prefix, if any)";
    public static final String OPT_DEST_PREFIX= "-d";
    public static final String LONGOPT_DEST_PREFIX = "--dest-prefix";
    @Option(name=OPT_DEST_PREFIX, aliases=LONGOPT_DEST_PREFIX, usage=USAGE_DEST_PREFIX)
    @Getter @Setter private String destPrefix = null;

    public boolean hasDestPrefix() { return destPrefix != null && destPrefix.length() > 0; }
    public int getDestPrefixLength () { return destPrefix == null ? 0 : destPrefix.length(); }

    public static final String USAGE_REGEX = "Only copy objects whose keys start match this regex";
    public static final String OPT_REGEX = "-R";
    public static final String LONGOPT_REGEX = "--regex";
    @Option(name=OPT_REGEX, aliases=LONGOPT_REGEX, usage=USAGE_REGEX)
    @Getter @Setter private String regex = null;

    public static final String USAGE_STRATEGY = "Choose the syncing strategy to be used to determine which objects should be copied.";
    public static final String OPT_STRATEGY = "-S";
    public static final String LONGOPT_STRATEGY = "--sync-strategy";
    @Option(name=OPT_STRATEGY, aliases=LONGOPT_STRATEGY, usage=USAGE_STRATEGY)
    @Getter @Setter private SyncStrategy syncStrategy = SyncStrategy.AUTO;

    public boolean hasRegex () { return regex != null && regex.length() > 0; }
    @Getter(lazy=true) private final Pattern regexPattern = initRegex();
    private Pattern initRegex() { return hasRegex() ? Pattern.compile(getRegex()) : null; }

    public static final String AWS_ENDPOINT = "AWS_ENDPOINT";

    public static final String USAGE_ENDPOINT = "AWS endpoint to use (or set "+AWS_ENDPOINT+" in your environment)";
    public static final String OPT_ENDPOINT = "-e";
    public static final String LONGOPT_ENDPOINT = "--endpoint";
    @Option(name=OPT_ENDPOINT, aliases=LONGOPT_ENDPOINT, usage=USAGE_ENDPOINT)
    @Builder.Default
    @Getter @Setter private String endpoint = System.getenv().get(AWS_ENDPOINT);

    public boolean hasEndpoint () { return endpoint != null && endpoint.trim().length() > 0; }

    public static final String USAGE_MAX_CONNECTIONS = "Maximum number of connections to S3 (default 100)";
    public static final String OPT_MAX_CONNECTIONS = "-m";
    public static final String LONGOPT_MAX_CONNECTIONS = "--max-connections";
    @Option(name=OPT_MAX_CONNECTIONS, aliases=LONGOPT_MAX_CONNECTIONS, usage=USAGE_MAX_CONNECTIONS)
    @Builder.Default
    @Getter @Setter private int maxConnections = 100;

    public static final String USAGE_MAX_THREADS = "Maximum number of threads (default 100)";
    public static final String OPT_MAX_THREADS = "-t";
    public static final String LONGOPT_MAX_THREADS = "--max-threads";
    @Option(name=OPT_MAX_THREADS, aliases=LONGOPT_MAX_THREADS, usage=USAGE_MAX_THREADS)
    @Builder.Default
    @Getter @Setter private int maxThreads = 100;

    public static final String USAGE_MAX_RETRIES = "Maximum number of retries for S3 requests (default 5)";
    public static final String OPT_MAX_RETRIES = "-r";
    public static final String LONGOPT_MAX_RETRIES = "--max-retries";
    @Option(name=OPT_MAX_RETRIES, aliases=LONGOPT_MAX_RETRIES, usage=USAGE_MAX_RETRIES)
    @Builder.Default
    @Getter @Setter private int maxRetries = 5;

    public static final String USAGE_CTIME = "Only copy objects whose Last-Modified date is younger than this many days. " +
            "For other time units, use these suffixes: y (years), M (months), d (days), w (weeks), h (hours), m (minutes), s (seconds)";
    public static final String OPT_CTIME = "-c";
    public static final String LONGOPT_CTIME = "--ctime";
    @Option(name=OPT_CTIME, aliases=LONGOPT_CTIME, usage=USAGE_CTIME)
    @Getter @Setter private String ctime = null;
    public boolean hasCtime() { return ctime != null; }

    private static final String PROXY_USAGE = "host:port of proxy server to use. " +
            "Defaults to proxy_host and proxy_port defined in ~/.s3cfg, or no proxy if these values are not found in ~/.s3cfg";
    public static final String OPT_PROXY = "-z";
    public static final String LONGOPT_PROXY = "--proxy";

    @Option(name=OPT_PROXY, aliases=LONGOPT_PROXY, usage=PROXY_USAGE)
    public void setProxy(String proxy) {
        final String[] splits = proxy.split(":");
        if (splits.length != 2) {
            throw new IllegalArgumentException("Invalid proxy setting ("+proxy+"), please use host:port");
        }

        proxyHost = splits[0];
        if (proxyHost.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid proxy setting ("+proxy+"), please use host:port");
        }
        try {
            proxyPort = Integer.parseInt(splits[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid proxy setting ("+proxy+"), port could not be parsed as a number");
        }
    }
    @Getter @Setter public String proxyHost = null;
    @Builder.Default
    @Getter @Setter public int proxyPort = -1;

    public boolean getHasProxy() { return proxyHost != null && proxyHost.trim().length() > 0; }

    private long initMaxAge() {

        DateTime dateTime = new DateTime(nowTime);

        // all digits -- assume "days"
        if (ctime.matches("^[0-9]+$")) return dateTime.minusDays(Integer.parseInt(ctime)).getMillis();

        // ensure there is at least one digit, and exactly one character suffix, and the suffix is a legal option
        if (!ctime.matches("^[0-9]+[yMwdhms]$")) throw new IllegalArgumentException("Invalid option for ctime: "+ctime);

        if (ctime.endsWith("y")) return dateTime.minusYears(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("M")) return dateTime.minusMonths(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("w")) return dateTime.minusWeeks(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("d")) return dateTime.minusDays(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("h")) return dateTime.minusHours(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("m")) return dateTime.minusMinutes(getCtimeNumber(ctime)).getMillis();
        if (ctime.endsWith("s")) return dateTime.minusSeconds(getCtimeNumber(ctime)).getMillis();
        throw new IllegalArgumentException("Invalid option for ctime: "+ctime);
    }

    private int getCtimeNumber(String ctime) {
        return Integer.parseInt(ctime.substring(0, ctime.length() - 1));
    }

    @Getter private long nowTime = System.currentTimeMillis();
    @Getter private long maxAge;
    @Getter private String maxAgeDate;

    public static final String USAGE_DELETE_REMOVED = "Delete objects from the destination bucket if they do not exist in the source bucket";
    public static final String OPT_DELETE_REMOVED = "-X";
    public static final String LONGOPT_DELETE_REMOVED = "--delete-removed";
    @Option(name=OPT_DELETE_REMOVED, aliases=LONGOPT_DELETE_REMOVED, usage=USAGE_DELETE_REMOVED)
    @Builder.Default
    @Getter @Setter private boolean deleteRemoved = false;

    @Argument(index=0, required=true, usage="source bucket[/source/prefix]") @Getter @Setter private String source;
    @Argument(index=1, required=true, usage="destination bucket[/dest/prefix]") @Getter @Setter private String destination;

    @Getter private String sourceBucket;
    @Getter private String destinationBucket;

    /**
     * Current max file size allowed in amazon is 5 GB. We can try and provide this as an option too.
     */
    public static final long MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE = 5 * GB;
    private static final long DEFAULT_PART_SIZE = 4 * GB;
    private static final String MULTI_PART_UPLOAD_SIZE_USAGE = "The upload size (in bytes) of each part uploaded as part of a multipart request " +
            "for files that are greater than the max allowed file size of " + MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE + " bytes ("+(MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE/GB)+"GB). " +
            "Defaults to " + DEFAULT_PART_SIZE + " bytes ("+(DEFAULT_PART_SIZE/GB)+"GB).";
    private static final String OPT_MULTI_PART_UPLOAD_SIZE = "-u";
    private static final String LONGOPT_MULTI_PART_UPLOAD_SIZE = "--upload-part-size";
    @Option(name=OPT_MULTI_PART_UPLOAD_SIZE, aliases=LONGOPT_MULTI_PART_UPLOAD_SIZE, usage=MULTI_PART_UPLOAD_SIZE_USAGE)
    @Builder.Default
    @Getter @Setter private long uploadPartSize = DEFAULT_PART_SIZE;

    private static final String CROSS_ACCOUNT_USAGE ="Copy across AWS accounts. Only Resource-based policies are supported (as " +
            "specified by AWS documentation) for cross account copying. " +
            "Default is false (copying within same account, preserving ACLs across copies). " +
            "If this option is active, we give full access to owner of the destination bucket.";
    private static final String OPT_CROSS_ACCOUNT_COPY = "-C";
    private static final String LONGOPT_CROSS_ACCOUNT_COPY = "--cross-account-copy";
    @Option(name=OPT_CROSS_ACCOUNT_COPY, aliases=LONGOPT_CROSS_ACCOUNT_COPY, usage=CROSS_ACCOUNT_USAGE)
    @Builder.Default
    @Getter @Setter private boolean crossAccountCopy = false;

    public void initDerivedFields() {

        if (hasCtime()) {
            this.maxAge = initMaxAge();
            this.maxAgeDate = new Date(maxAge).toString();
        }

        final BucketAndPrefix src = new BucketAndPrefix(source, prefix);
        sourceBucket = src.bucket;
        prefix = src.prefix;
        if (verbose) log.info("src="+src);

        final BucketAndPrefix dest = new BucketAndPrefix(destination, destPrefix);
        destinationBucket = dest.bucket;
        destPrefix = dest.prefix;
        if (verbose) log.info("dest="+dest);
    }

    public void apply(InitiateMultipartUploadRequest request) {
        request.setStorageClass(StorageClass.valueOf(getStorageClass().name()));
        if (isEncrypt()) request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
    }

    public void apply(PutObjectRequest request) {
        request.setStorageClass(StorageClass.valueOf(getStorageClass().name()));
        if (isEncrypt()) request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
    }

    public void apply(UploadPartRequest request) {
        if (isEncrypt()) request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
    }

    public void apply(CopyObjectRequest request) {
        request.setStorageClass(StorageClass.valueOf(getStorageClass().name()));
        if (isEncrypt()) request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
    }

    // Consolidates logic around parsing source/destination as buckets/folders/prefixes
    @ToString
    static class BucketAndPrefix {
        public String bucket;
        public String prefix;

        protected String scrubS3ProtocolPrefix(String bucket) {
            bucket = bucket.trim();
            if (bucket.startsWith(S3_PROTOCOL_PREFIX)) {
                bucket = bucket.substring(S3_PROTOCOL_PREFIX.length());
            }
            return bucket;
        }

        public BucketAndPrefix (String path, String pfx) {

            final String scrubbed = scrubS3ProtocolPrefix(path);
            String slash = FileStoreFactory.findSlash(path, pfx);
            if (slash == null) slash = File.separator;

            final int slashPos = slashPos(scrubbed);
            final int hereDir = scrubbed.indexOf("." + slash);

            if (FileStoreFactory.isLocalPath(path)) {
                if (path.equals(READ_FILES_FROM_STDIN)) {
                    bucket = path;
                    prefix = null;
                } else {
                    if (hereDir == 0) {
                        // replace ./ with current directory name
                        bucket = new File(System.getProperty("user.dir") + (scrubbed.length() > 2 ? slash + scrubbed.substring(2) : "")).getAbsolutePath();
                    } else {
                        bucket = scrubbed;
                    }

                    // if the path is not to a dir, then assume anything after the last / should be pre-pended to the prefix
                    final File bucketDir = new File(bucket);
                    if (!bucketDir.isDirectory()) {
                        bucket = bucketDir.getParentFile().getAbsolutePath();
                        prefix = pfx == null || pfx.trim().isEmpty()
                                ? bucketDir.getName()
                                : bucketDir.getName() + slash + pfx;
                    } else {
                        prefix = pfx;
                    }
                    if (!bucket.endsWith(slash)) bucket += slash;
                }

            } else if (slashPos != -1) {
                // this is for S3, in the form "bucket/prefix"
                bucket = scrubbed.substring(0, slashPos);
                if (slashPos < scrubbed.length()-1) {
                    prefix = pfx == null || pfx.trim().isEmpty()
                            ? scrubbed.substring(slashPos+1)
                            : scrubbed.substring(slashPos+1) + pfx;
                } else {
                    prefix = pfx;
                }

            } else {
                bucket = scrubbed;
                prefix = pfx;
            }

            if (prefix != null && prefix.trim().length() == 0) prefix = null;
        }

        private int slashPos(String scrubbed) {
            final int s1 = scrubbed.indexOf('/');
            final int s2 = scrubbed.indexOf('\\');
            return s1 == -1 ? s2 : (s2 == -1) ? s1 : (s1 < s2) ? s1 : s2;
        }
    }

}
