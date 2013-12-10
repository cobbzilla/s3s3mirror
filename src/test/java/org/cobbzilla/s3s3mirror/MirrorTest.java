package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Slf4j
public class MirrorTest {

    public static final String SOURCE_ENV_VAR = "S3S3_TEST_SOURCE";
    public static final String DEST_ENV_VAR = "S3S3_TEST_DEST";

    public static final String SOURCE = System.getenv(SOURCE_ENV_VAR);
    public static final String DESTINATION = System.getenv(DEST_ENV_VAR);
    public static final int TEST_FILE_SIZE = 1024;

    private List<S3Asset> stuffToCleanup = new ArrayList<S3Asset>();

    @After
    public void cleanupS3Assets () {
        MirrorMain mirrorMain = new MirrorMain(new String[]{SOURCE, DESTINATION});
        mirrorMain.init();
        AmazonS3Client client = mirrorMain.getClient();
        for (S3Asset asset : stuffToCleanup) {
            try {
                client.deleteObject(asset.bucket, asset.key);
            } catch (Exception e) {
                log.error("Error cleaning up object: "+asset);
            }
        }
    }

    @Test
    public void testSimpleCopy () throws Exception {
        if (!checkEnvs()) return;

        final String key = "testkey_"+random(10);

        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_VERBOSE, MirrorOptions.OPT_PREFIX, key, SOURCE, DESTINATION});
        main.init();

        final TestFile testFile = new TestFile();
        stuffToCleanup.add(new S3Asset(SOURCE, key));
        stuffToCleanup.add(new S3Asset(DESTINATION, key));
        main.getClient().putObject(SOURCE, key, testFile.file);

        main.run();
        assertEquals(1, main.getContext().getStats().objectsCopied.get());
        assertEquals(testFile.data.length(), main.getContext().getStats().bytesCopied.get());

        final ObjectMetadata metadata = main.getClient().getObjectMetadata(DESTINATION, key);
        assertEquals(testFile.data.length(), metadata.getContentLength());
    }

    @AllArgsConstructor @ToString
    class S3Asset {
        public String bucket;
        public String key;
    }

    class TestFile {
        public File file;
        public String data;
        public TestFile () throws Exception{
            file = File.createTempFile(getClass().getName(), ".tmp");
            data = random(TEST_FILE_SIZE);
            @Cleanup FileOutputStream out = new FileOutputStream(file);
            IOUtils.copy(new ByteArrayInputStream(data.getBytes()), out);
            file.deleteOnExit();
        }
    }

    private String random(int size) {
        return RandomStringUtils.randomAlphanumeric(size) + "_" + System.currentTimeMillis();
    }

    private boolean checkEnvs() {
        if (SOURCE == null || DESTINATION == null) {
            log.warn("No "+SOURCE_ENV_VAR+" and/or no "+DEST_ENV_VAR+" found in enviroment, skipping test");
            return false;
        }
        return true;
    }

}
