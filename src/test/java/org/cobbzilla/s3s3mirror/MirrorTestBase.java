package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.cobbzilla.s3s3mirror.store.s3.S3ClientService;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MirrorTestBase {

    public static final String SOURCE_ENV_VAR = "S3S3_TEST_SOURCE";
    public static final String DEST_ENV_VAR = "S3S3_TEST_DEST";

    public static final String LOCAL_TO_S3_DESTINATION_ENV_VAR = "LOCAL_TO_S3S3_TEST_DEST";
    public static final String LOCAL_TO_S3_DESTINATION = System.getenv(LOCAL_TO_S3_DESTINATION_ENV_VAR);

    public static final String SOURCE = System.getenv(SOURCE_ENV_VAR);
    public static final String DESTINATION = System.getenv(DEST_ENV_VAR);

    protected List<S3Asset> stuffToCleanup = new ArrayList<S3Asset>();

    // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.
    protected MirrorMain main = null;

    protected AmazonS3Client getS3Client() { return S3ClientService.getS3Client(main.getOptions()); }

    protected File localDir;
    @Before public void initLocalDir() throws Exception { localDir = Files.createTempDirectory(null).toFile(); }
    @After public void deleteLocalDir() throws Exception { FileUtils.deleteDirectory(localDir); }

    protected File createLocalTestFile(String destKey) throws Exception {
        return createLocalTestFile(localDir.getAbsolutePath(), destKey);
    }

    protected TestFile createTestFile(String key, TestFile.Copy copy, TestFile.Clean clean) throws Exception {
        return TestFile.create(key, getS3Client(), stuffToCleanup, copy, clean);
    }

    protected File createLocalTestFile(String base, String name) throws Exception {
        final File localFile = new File(base + File.separator + name);
        FileUtils.write(localFile, MirrorTest.random(TestFile.TEST_FILE_SIZE + (RandomUtils.nextInt() % 1024)));
        return localFile;
    }

    public static String random(int size) {
        return RandomStringUtils.randomAlphanumeric(size) + "_" + System.currentTimeMillis();
    }

    protected boolean checkEnvs() {
        if (SOURCE == null || DESTINATION == null) {
            log.warn("No "+SOURCE_ENV_VAR+" and/or no "+DEST_ENV_VAR+" found in enviroment, skipping test");
            return false;
        }
        return true;
    }

    @After
    public void cleanupS3Assets () {
        // Every individual test *must* initialize the "main" instance variable, otherwise NPE gets thrown here.
        if (checkEnvs()) {
            AmazonS3Client client = getS3Client();
            for (S3Asset asset : stuffToCleanup) {
                try {
                    log.info("cleanupS3Assets: deleting "+asset);
                    client.deleteObject(asset.bucket, asset.key);
                } catch (Exception e) {
                    log.error("Error cleaning up object: "+asset+": "+e.getMessage());
                }
            }
            main = null;
        }
    }

}
