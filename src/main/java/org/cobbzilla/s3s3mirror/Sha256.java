package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.Cleanup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

//  todo: generify, allow swappable hash function instead of hard-coded sha256
public class Sha256 {

    public static ObjectMetadata getS3MetadataWithHash(File srcFile) {
        final ObjectMetadata metadata = new ObjectMetadata();
        final Map<String, String> userMeta = userMetaWithHash(srcFile);
        metadata.setUserMetadata(userMeta);
        return metadata;
    }

    public static Map<String, String> userMetaWithHash(File srcFile) {
        final Map<String, String> userMeta = new HashMap<>();
        userMeta.put(S3S3_SHA256, hash(srcFile));
        return userMeta;
    }

    /** key in s3 object metadata, used when copying to/from local because ETag is not calculable by your local filesystem */
    public static final String S3S3_SHA256 = "content-sha256";

    /** Special value set by KeyLister to indicate the value should be filled in from ObjectMetadata read by KeyJob.
     *  This is needed because when listing keys, the UserMetadata where the SHA-256 is stored is inaccessible. */
    public static final String CHECK_OBJECT_METADATA = "CHECK_OBJECT_METADATA";

    private static MessageDigest md() throws NoSuchAlgorithmException { return MessageDigest.getInstance("SHA-256"); }

    public static MessageDigest getMessageDigest(InputStream input) throws NoSuchAlgorithmException, IOException, DigestException {
        final byte[] buf = new byte[4096];
        final MessageDigest md = md();
        while (true) {
            int read = input.read(buf, 0, buf.length);
            if (read == -1) break;
            md.update(buf, 0, read);
        }
        return md;
    }
    public static byte[] hash (byte[] data) {
        if (data == null) throw new NullPointerException("sha256: null argument");
        try {
            return md().digest(data);
        } catch (Exception e) {
            throw new IllegalStateException("sha256: bad data: "+e, e);
        }
    }

    public static String hash (File file) {
        try {
            @Cleanup final InputStream input = new FileInputStream(file);
            final MessageDigest md = getMessageDigest(input);
            return tohex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException("Error calculating sha256 on " + file.getAbsolutePath() + ": " + e);
        }
    }

    public static String tohex(byte[] data) { return tohex(data, 0, data.length); }

    public static String tohex(byte[] data, int start, int len) {
        StringBuilder b = new StringBuilder();
        int stop = start+len;
        for (int i=start; i<stop; i++) {
            b.append(getHexValue(data[i]));
        }
        return b.toString();
    }
    public static final String[] HEX_DIGITS = {"0", "1", "2", "3", "4", "5", "6", "7",
                                               "8", "9", "a", "b", "c", "d", "e", "f"};

    /**
     * Get the hexadecimal string representation for a byte.
     * The leading 0x is not included.
     *
     * @param b the byte to process
     * @return a String representing the hexadecimal value of the byte
     */
    public static String getHexValue(byte b) {
        int i = (int) b;
        return HEX_DIGITS[((i >> 4) + 16) % 16] + HEX_DIGITS[(i + 128) % 16];
    }
}
