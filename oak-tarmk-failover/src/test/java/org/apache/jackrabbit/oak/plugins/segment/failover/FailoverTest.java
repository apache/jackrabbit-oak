package org.apache.jackrabbit.oak.plugins.segment.failover;

import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.client.FailoverClient;
import org.apache.jackrabbit.oak.plugins.segment.failover.server.FailoverServer;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverTest {

    private int port = Integer.valueOf(System.getProperty(
            "failover.server.port", "52808"));

    private File directoryS;

    private FileStore storeS;

    private File directoryC;

    private FileStore storeC;

    @Before
    public void setUp() throws IOException {
        File target = new File("target");

        // server
        directoryS = createTempFile("FailoverServerTest", "dir", target);
        directoryS.delete();
        directoryS.mkdir();
        storeS = new FileStore(directoryS, 1, false);

        // client
        directoryC = createTempFile("FailoverClientTest", "dir", target);
        directoryC.delete();
        directoryC.mkdir();
        storeC = new FileStore(directoryC, 1, false);
    }

    @After
    public void after() {
        storeS.close();
        storeC.close();
        try {
            FileUtils.deleteDirectory(directoryS);
            FileUtils.deleteDirectory(directoryC);
        } catch (IOException e) {
        }
    }

    @Test
    public void testFailover() throws Exception {

        NodeStore store = new SegmentNodeStore(storeS);
        final FailoverServer server = new FailoverServer(port, storeS);
        Thread s = new Thread() {
            public void run() {
                server.start();
            }
        };
        s.start();
        addTestContent(store);

        FailoverClient cl = new FailoverClient("127.0.0.1", port, storeC);
        cl.run();

        try {
            assertEquals(storeS.getHead(), storeC.getHead());
        } finally {
            server.close();
            cl.close();
        }

    }

    private static void addTestContent(NodeStore store)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("server").setProperty("ts", System.currentTimeMillis());
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    static void assertEqualStores(File d1, File d2) throws IOException {
        FileStore f1 = new FileStore(d1, 1, false);
        FileStore f2 = new FileStore(d2, 1, false);
        try {
            assertEquals(f1.getHead(), f2.getHead());
        } finally {
            f1.close();
            f2.close();
        }
    }

    public static void main(String[] args) throws Exception {
        File d = createTempFile("FailoverLiveTest", "dir", new File("target"));
        d.delete();
        d.mkdir();
        FileStore s = new FileStore(d, 256, false);
        FailoverClient cl = new FailoverClient("127.0.0.1", 8023, s);
        try {
            cl.run();
        } finally {
            s.close();
            cl.close();
        }
    }

}
