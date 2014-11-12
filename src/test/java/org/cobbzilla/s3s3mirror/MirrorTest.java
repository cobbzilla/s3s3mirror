package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.cobbzilla.s3s3mirror.MirrorOptions.*;
import static org.cobbzilla.s3s3mirror.TestFile.Clean;
import static org.cobbzilla.s3s3mirror.TestFile.Copy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class MirrorTest extends MirrorTestBase {

    @Test
    public void testSimpleCopy () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopy_"+random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, key, SOURCE, DESTINATION};

        testSimpleCopyInternal(key, args);
    }

    @Test
    public void testSimpleCopyWithInlinePrefix () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlinePrefix_"+random(10);
        final String[] args = {OPT_VERBOSE, SOURCE + "/" + key, DESTINATION};

        testSimpleCopyInternal(key, args);
    }

    private void testSimpleCopyInternal(String key, String[] args) throws Exception {

        main = new MirrorMain(args);
        main.init();

        final TestFile testFile = createTestFile(key, Copy.SOURCE, Clean.SOURCE_AND_DEST);

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(testFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final ObjectMetadata metadata = getS3Client().getObjectMetadata(DESTINATION, key);
        assertEquals(testFile.data.length(), metadata.getContentLength());
    }

    @Test
    public void testSimpleCopyWithDestPrefix () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithDestPrefix_"+random(10);
        final String destKey = "dest_testSimpleCopyWithDestPrefix_"+random(10);
        final String[] args = {OPT_PREFIX, key, OPT_DEST_PREFIX, destKey, SOURCE, DESTINATION};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    @Test
    public void testSimpleCopyWithInlineDestPrefix () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlineDestPrefix_"+random(10);
        final String destKey = "dest_testSimpleCopyWithInlineDestPrefix_"+random(10);
        final String[] args = {SOURCE+"/"+key, DESTINATION+"/"+destKey };
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    private void testSimpleCopyWithDestPrefixInternal(String key, String destKey, String[] args) throws Exception {
        main = new MirrorMain(args);
        main.init();

        final TestFile testFile = createTestFile(key, Copy.SOURCE, Clean.SOURCE);
        stuffToCleanup.add(new S3Asset(DESTINATION, destKey));

        main.run();

        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(testFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final ObjectMetadata metadata = getS3Client().getObjectMetadata(DESTINATION, destKey);
        assertEquals(testFile.data.length(), metadata.getContentLength());
    }

    @Test
    public void testDeleteRemoved () throws Exception {
        if (!checkEnvs()) return;

        final String key = "testDeleteRemoved_"+random(10);

        main = new MirrorMain(new String[]{OPT_VERBOSE, OPT_PREFIX, key,
                                           OPT_DELETE_REMOVED, SOURCE, DESTINATION+"/"+key});
        main.init();

        // Write some files to dest
        final int numDestFiles = 3;
        final String[] destKeys = new String[numDestFiles];
        final TestFile[] destFiles = new TestFile[numDestFiles];
        for (int i=0; i<numDestFiles; i++) {
            destKeys[i] = key + "-dest" + i;
            destFiles[i] = createTestFile(destKeys[i], Copy.DEST, Clean.DEST);
        }

        // Write 1 file to source
        final String srcKey = key + "-src";
        final TestFile srcFile = createTestFile(srcKey, Copy.SOURCE, Clean.SOURCE_AND_DEST);

        // Initiate copy
        main.run();

        // Expect only 1 copy and numDestFiles deletes
        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(numDestFiles, main.getContext().getStats().objectsDeleted.get());

        // Expect none of the original dest files to be there anymore
        for (int i=0; i<numDestFiles; i++) {
            try {
                getS3Client().getObjectMetadata(DESTINATION, destKeys[i]);
                fail("testDeleteRemoved: expected "+destKeys[i]+" to be removed from destination bucket "+DESTINATION);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    fail("testDeleteRemoved: unexpected exception (expected statusCode == 404): "+e);
                }
            }
        }

        // Expect source file to now be present in both source and destination buckets
        ObjectMetadata metadata;
        metadata = getS3Client().getObjectMetadata(SOURCE, srcKey);
        assertEquals(srcFile.data.length(), metadata.getContentLength());

        metadata = getS3Client().getObjectMetadata(DESTINATION, srcKey);
        assertEquals(srcFile.data.length(), metadata.getContentLength());
    }

}
