package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Test;

import java.io.File;

import static org.cobbzilla.s3s3mirror.MirrorOptions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class S3ToLocalTest extends MirrorTestBase {

    @Test
    public void testSimpleCopy () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopy_"+random(10);
        final String[] args = {OPT_VERBOSE, OPT_PREFIX, key, SOURCE, localDir.getAbsolutePath() };

        testSimpleCopyInternal(key, args);
    }

    @Test
    public void testSimpleCopyWithInlinePrefix () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithInlinePrefix_"+random(10);
        final String[] args = {OPT_VERBOSE, SOURCE + "/" + key, localDir.getAbsolutePath()};

        testSimpleCopyInternal(key, args);
    }

    private void testSimpleCopyInternal(String key, String[] args) throws Exception {

        main = new MirrorMain(args);
        main.init();

        final TestFile testFile = createTestFile(key, TestFile.Copy.SOURCE, TestFile.Clean.SOURCE);

        main.run();

        assertEquals(testFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final File destFile = new File(localDir.getAbsolutePath() + File.separator + testFile.key);
        assertEquals(testFile.data.length(), destFile.length());
    }

    @Test
    public void testSimpleCopyWithDestPrefix () throws Exception {
        if (!checkEnvs()) return;
        final String key = "testSimpleCopyWithDestPrefix_"+random(10);
        final String destKey = "dest_testSimpleCopyWithDestPrefix_"+random(10);
        final String[] args = {OPT_PREFIX, key, OPT_DEST_PREFIX, destKey, SOURCE, localDir.getAbsolutePath()};
        testSimpleCopyWithDestPrefixInternal(key, destKey, args);
    }

    private void testSimpleCopyWithDestPrefixInternal(String key, String destKey, String[] args) throws Exception {
        main = new MirrorMain(args);
        main.init();

        final TestFile testFile = createTestFile(key, TestFile.Copy.SOURCE, TestFile.Clean.SOURCE);

        main.run();

        final File destFile = new File(localDir.getAbsolutePath() + File.separator +  destKey);
        assertEquals(testFile.data.length(), destFile.length());
    }

    @Test
    public void testDeleteRemoved () throws Exception {
        if (!checkEnvs()) return;

        final String key = "testDeleteRemoved_"+random(10);

        main = new MirrorMain(new String[]{OPT_VERBOSE, OPT_PREFIX, key, OPT_DELETE_REMOVED, SOURCE, localDir.getAbsolutePath()});
        main.init();

        // Write some files to dest
        final int numDestFiles = 3;
        final String[] destKeys = new String[numDestFiles];
        final File[] destFiles = new File[numDestFiles];
        for (int i=0; i<numDestFiles; i++) {
            destKeys[i] = key + "-dest" + i;
            destFiles[i] = createLocalTestFile(destKeys[i]);
        }

        // Write 1 file to source
        final String srcKey = key + "-src";
        final TestFile srcFile = createTestFile(srcKey, TestFile.Copy.SOURCE, TestFile.Clean.SOURCE_AND_DEST);

        // Initiate copy
        main.run();

        // Expect only 1 copy and numDestFiles deletes
        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(numDestFiles, main.getContext().getStats().objectsDeleted.get());

        // Expect none of the original dest files to be there anymore
        for (int i=0; i<numDestFiles; i++) {
            assertFalse("testDeleteRemoved: expected " + destFiles[i] + " to be removed", destFiles[i].exists());
        }

        // Expect source file to now be present in both source bucket and destination directory
        final ObjectMetadata metadata = getS3Client().getObjectMetadata(SOURCE, srcKey);
        assertEquals(srcFile.data.length(), metadata.getContentLength());
        assertEquals(new File(localDir.getAbsolutePath() + File.separator + srcKey).length(), metadata.getContentLength());
    }

}
