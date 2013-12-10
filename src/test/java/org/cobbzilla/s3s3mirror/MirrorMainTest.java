package org.cobbzilla.s3s3mirror;

import org.junit.Test;

import static org.junit.Assert.*;

public class MirrorMainTest {

    public static final String SOURCE = "s3://from-bucket";
    public static final String DESTINATION = "s3://to-bucket";

    @Test
    public void testBasicArgs () throws Exception {

        final MirrorMain main = new MirrorMain(new String[]{SOURCE, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertFalse(options.isDryRun());
        assertEquals(SOURCE, options.getSource());
        assertEquals(DESTINATION, options.getDestination());
    }

    @Test
    public void testDryRunArgs () throws Exception {

        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_DRY_RUN, SOURCE, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertTrue(options.isDryRun());
        assertEquals(SOURCE, options.getSource());
        assertEquals(DESTINATION, options.getDestination());
    }

    @Test
    public void testMaxConnectionsArgs () throws Exception {

        int maxConns = 42;
        final MirrorMain main = new MirrorMain(new String[]{MirrorOptions.OPT_MAX_CONNECTIONS, String.valueOf(maxConns), SOURCE, DESTINATION});
        main.parseArguments();

        final MirrorOptions options = main.getOptions();
        assertFalse(options.isDryRun());
        assertEquals(maxConns, options.getMaxConnections());
        assertEquals(SOURCE, options.getSource());
        assertEquals(DESTINATION, options.getDestination());
    }
}
