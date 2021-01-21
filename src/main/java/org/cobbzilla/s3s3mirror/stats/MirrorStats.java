package org.cobbzilla.s3s3mirror.stats;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.KeyJob;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.cobbzilla.s3s3mirror.MirrorConstants.*;

@Slf4j
public class MirrorStats {
    public final AtomicLong objectsRead = new AtomicLong(0);
    public final AtomicLong objectsCopied = new AtomicLong(0);
    private final AtomicLong copyErrors = new AtomicLong(0);
    public final AtomicLong objectsPut = new AtomicLong(0);
    public final AtomicLong objectsDeleted = new AtomicLong(0);
    private final AtomicLong deleteErrors = new AtomicLong(0);

    public final AtomicLong s3copyCount = new AtomicLong(0);
    public final AtomicLong s3putCount = new AtomicLong(0);
    public final AtomicLong s3deleteCount = new AtomicLong(0);
    public final AtomicLong s3getCount = new AtomicLong(0);
    public final AtomicLong bytesCopied = new AtomicLong(0);
    public final AtomicLong bytesUploaded = new AtomicLong(0);

    public static final long HOUR = TimeUnit.HOURS.toMillis(1);
    public static final long MINUTE = TimeUnit.MINUTES.toMillis(1);
    public static final long SECOND = TimeUnit.SECONDS.toMillis(1);
    public final AtomicBoolean completedFully = new AtomicBoolean(true);
    public final AtomicBoolean logFailedOperationInfo = new AtomicBoolean(false);
    @Getter
    private final Set<FailedOperation> failedCopies = ConcurrentHashMap.newKeySet();
    @Getter
    private final Set<FailedOperation> failedDeletes = ConcurrentHashMap.newKeySet();
    private long start = System.currentTimeMillis();

    @Getter private final Thread shutdownHook = new Thread(this::logStats);

    private static final String BANNER = "\n--------------------------------------------------------------------\n";
    public void logStats() {
        log.info(BANNER + "STATS BEGIN\n" + toString() + "STATS END " + BANNER);
    }

    public String toString () {
        final long durationMillis = System.currentTimeMillis() - start;
        final double durationMinutes = durationMillis / 60000.0d;
        final String duration = String.format("%d:%02d:%02d", durationMillis / HOUR, (durationMillis % HOUR) / MINUTE, (durationMillis % MINUTE) / SECOND);
        final double readRate = objectsRead.get() / durationMinutes;
        final double copyRate = (objectsCopied.get() + objectsPut.get()) / durationMinutes;
        final double deleteRate = objectsDeleted.get() / durationMinutes;
        return "read: "+objectsRead+ "\n"
                + "copied: "+objectsCopied+"\n"
                + "copy errors: "+copyErrors+"\n"
                + "uploaded: "+objectsPut+"\n"
                + "deleted: "+objectsDeleted+"\n"
                + "delete errors: "+deleteErrors+"\n"
                + "duration: "+duration+"\n"
                + "read rate: "+readRate+"/minute\n"
                + "copy+upload rate: "+copyRate+"/minute\n"
                + "delete rate: "+deleteRate+"/minute\n"
                + "bytes copied: "+formatBytes(bytesCopied.get())+"\n"
                + "bytes uploaded: "+formatBytes(bytesUploaded.get())+"\n"
                + "GET operations: "+s3getCount+"\n"
                + "COPY operations: "+ s3copyCount+"\n"
                + "PUT operations: "+ s3putCount+"\n"
                + "DELETE operations: "+ s3deleteCount+"\n";
    }

    private String formatBytes(long bytesCopied) {
        if (bytesCopied > EB) return ((double) bytesCopied) / ((double) EB) + " EB ("+bytesCopied+" bytes)";
        if (bytesCopied > PB) return ((double) bytesCopied) / ((double) PB) + " PB ("+bytesCopied+" bytes)";
        if (bytesCopied > TB) return ((double) bytesCopied) / ((double) TB) + " TB ("+bytesCopied+" bytes)";
        if (bytesCopied > GB) return ((double) bytesCopied) / ((double) GB) + " GB ("+bytesCopied+" bytes)";
        if (bytesCopied > MB) return ((double) bytesCopied) / ((double) MB) + " MB ("+bytesCopied+" bytes)";
        if (bytesCopied > KB) return ((double) bytesCopied) / ((double) KB) + " KB ("+bytesCopied+" bytes)";
        return bytesCopied + " bytes";
    }

    public void addErroredCopy(KeyJob failedObject) {
        copyErrors.incrementAndGet();
        if (logFailedOperationInfo.get()) {
            failedCopies.add(new FailedOperation(failedObject.getSource(), failedObject.getDestination()));
        }
    }

    public void addErroredDelete(KeyJob failedObject) {
        deleteErrors.incrementAndGet();
        if (logFailedOperationInfo.get()) {
            failedDeletes.add(new FailedOperation(failedObject.getSource(), failedObject.getDestination()));
        }
    }

    public MirrorStats copy() {
        MirrorStats copied = new MirrorStats();
        copied.objectsRead.set(objectsRead.get());
        copied.objectsCopied.set(objectsCopied.get());
        copied.objectsDeleted.set(objectsDeleted.get());
        copied.objectsPut.set(objectsPut.get());
        copied.copyErrors.set(copyErrors.get());
        copied.deleteErrors.set(deleteErrors.get());
        copied.s3copyCount.set(s3copyCount.get());
        copied.s3putCount.set(s3putCount.get());
        copied.s3deleteCount.set(s3deleteCount.get());
        copied.s3getCount.set(s3getCount.get());
        copied.bytesCopied.set(bytesCopied.get());
        copied.bytesUploaded.set(bytesUploaded.get());
        copied.completedFully.set(completedFully.get());
        copied.logFailedOperationInfo.set(logFailedOperationInfo.get());
        copied.failedCopies.addAll(failedCopies);
        copied.failedDeletes.addAll(failedDeletes);
        copied.start = start;

        return copied;
    }
}
