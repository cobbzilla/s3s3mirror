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
                + "copy rate: "+copyRate+"/minute\n";
    }
}
