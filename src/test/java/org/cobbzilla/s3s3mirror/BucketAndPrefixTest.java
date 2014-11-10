package org.cobbzilla.s3s3mirror;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.cobbzilla.s3s3mirror.MirrorOptions.BucketAndPrefix;
import static org.junit.Assert.assertEquals;

public class BucketAndPrefixTest {

    public String[][] S3_TESTS = new String[][] {
            { "bucket1", null, "bucket1", null },
            { "bucket2/", null, "bucket2", null },

            { "bucket3", "prefix", "bucket3", "prefix" },
            { "bucket4/", "prefix", "bucket4", "prefix" },

            { "bucket5/prefix", null, "bucket5", "prefix" },
            { "bucket6/prefix/", null, "bucket6", "prefix/" },

            { "bucket7/prefix", "pre2", "bucket7", "prefixpre2" },
            { "bucket8/prefix/", "pre2", "bucket8", "prefix/pre2" },

            { "bucket9/prefix/pre2", "pre3/pre4", "bucket9", "prefix/pre2pre3/pre4" },
            { "bucket10/prefix/pre2/", "pre3/pre4/", "bucket10", "prefix/pre2/pre3/pre4/" },
    };

    @Test
    public void testS3Paths () throws Exception {
        BucketAndPrefix bucketAndPrefix;
        for (String test[] : S3_TESTS) {
            bucketAndPrefix = new BucketAndPrefix(test[0], test[1]);
            assertEquals(test[2], bucketAndPrefix.bucket);
            assertEquals(test[3], bucketAndPrefix.prefix);

            bucketAndPrefix = new BucketAndPrefix("s3://"+test[0], test[1]);
            assertEquals(test[2], bucketAndPrefix.bucket);
            assertEquals(test[3], bucketAndPrefix.prefix);
        }
    }

    private File rootDir;
    private File relativeDir;

    @Before public void initTempDirs () throws Exception {
        rootDir = Files.createTempDirectory(null).toFile();
        relativeDir = new File(System.getProperty("user.dir"));
        for (String dir : TEST_DIRS) {
            if (!new File(rootDir + File.separator + dir).mkdirs()) throw new IllegalStateException("Error creating dir: "+dir);
            if (!new File(relativeDir.getAbsolutePath() + File.separator + dir).mkdirs()) throw new IllegalStateException("Error creating dir: "+dir);
        }
    }

    @After public void deleteTempDirs () throws Exception {
        FileUtils.deleteDirectory(rootDir);
        for (String dir : TEST_DIRS) {
            FileUtils.deleteDirectory(new File(relativeDir + File.separator + dir));
        }
    }

    public String[][] FILE_TESTS = new String[][] {
            { "./", null, "@rel/", null },
            { "./", "", "@rel/", null },

            { "./path1", null, "@rel/path1/", null },
            { "./path1", "path2", "@rel/path1/", "path2" },

            { "./path1/foo", null, "@rel/path1/", "foo" },
            { "./path1/foo", "path2", "@rel/path1/", "foo/path2" },

            { "@root/path1/path2", null, "@root/path1/path2/", null },
            { "@root/path1/path2", "", "@root/path1/path2/", null },

            { "@root/path1", "path2", "@root/path1/", "path2" },
            { "@root/path1/foo", "path2", "@root/path1/", "foo/path2" },
            { "@root/path1", "path2/foo", "@root/path1/", "path2/foo" },
    };

    public String[] TEST_DIRS = new String[] {
            "path1/path2"
    };

    @Test
    public void testFilePaths () throws Exception {
        BucketAndPrefix bucketAndPrefix;
        for (String test[] : FILE_TESTS) {
            bucketAndPrefix = new BucketAndPrefix(replace(test[0]), test[1]);
            assertEquals(replace(test[2]), bucketAndPrefix.bucket);
            assertEquals(test[3], bucketAndPrefix.prefix);
        }
    }

    private String replace(String s) {
        return s.replace("@root", rootDir.getAbsolutePath()).replace("@rel", relativeDir.getAbsolutePath());
    }

}
