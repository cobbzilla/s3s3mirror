package org.cobbzilla.s3s3mirror;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class MirrorStats {

    public Thread getShutdownHook() {
        return new Thread() {
            @Override public void run() { logStats(); }
        };
    }

    private static final String BANNER = "\n--------------------------------------------------------------------\n";
    public void logStats() {
        log.info(BANNER + "STATS BEGIN\n" + toString() + "STATS END " + BANNER);
    }

    private long start = System.currentTimeMillis();

    public volatile long objectsRead;
    public volatile long objectsCopied;
    public volatile long copyErrors;

    public volatile long s3opCount;
    public volatile long bytesCopied;

    public static final long HOUR = TimeUnit.HOURS.toMillis(1);
    public static final long MINUTE = TimeUnit.MINUTES.toMillis(1);
    public static final long SECOND = TimeUnit.SECONDS.toMillis(1);

    public String toString () {
        final long durationMillis = System.currentTimeMillis() - start;
        final double durationMinutes = durationMillis / 60000.0d;
        final String duration = String.format("%d:%02d:%02d", durationMillis / HOUR, (durationMillis % HOUR) / MINUTE, (durationMillis % MINUTE) / SECOND);
        final double readRate = objectsRead / durationMinutes;
        final double copyRate = objectsCopied / durationMinutes;
        return "read: "+objectsRead + "\n"
                + "copied: "+objectsCopied+"\n"
                + "copy errors: "+copyErrors+"\n"
                + "duration: "+duration+"\n"
                + "read rate: "+readRate+"/minute\n"
                + "copy rate: "+copyRate+"/minute\n"
                + "bytes copied: "+formatBytes(bytesCopied)+"\n"
                + "S3 operations: "+s3opCount+"\n";
    }

    public static final long KB = 1024;
    public static final long MB = KB * 1024;
    public static final long GB = MB * 1024;
    public static final long PB = GB * 1024;
    public static final long EB = PB * 1024;

    private String formatBytes(long bytesCopied) {
        if (bytesCopied > EB) return ((double) bytesCopied) / ((double) EB) + "EB ("+bytesCopied+" bytes)";
        if (bytesCopied > PB) return ((double) bytesCopied) / ((double) PB) + "PB ("+bytesCopied+" bytes)";
        if (bytesCopied > GB) return ((double) bytesCopied) / ((double) GB) + "GB ("+bytesCopied+" bytes)";
        if (bytesCopied > MB) return ((double) bytesCopied) / ((double) MB) + "MB ("+bytesCopied+" bytes)";
        if (bytesCopied > KB) return ((double) bytesCopied) / ((double) KB) + "KB ("+bytesCopied+" bytes)";
        return bytesCopied + " bytes";
    }

}
