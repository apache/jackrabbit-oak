/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.standby;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.proxy.NetworkErrorProxy;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataStoreTestBase extends TestBase {

    private static final Logger logger = LoggerFactory.getLogger(DataStoreTestBase.class);

    private static final long GB = 1024 * 1024 * 1024;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public TemporaryPort proxyPort = new TemporaryPort();

    @Rule
    public TestName testName = new TestName();

    abstract FileStore getPrimary();

    abstract FileStore getSecondary();

    abstract boolean storesShouldBeDifferent();

    private InputStream newRandomInputStream(final long size, final int seed) {
        return new InputStream() {

            private final Random random = new Random(seed);

            private long count = 0;

            @Override
            public int read() throws IOException {
                if (count >= size) {
                    return -1;
                }
                count++;
                return Math.abs(random.nextInt());
            }

        };
    }

    protected byte[] addTestContent(NodeStore store, String child, int size)
        throws CommitFailedException, IOException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(child).setProperty("ts", System.currentTimeMillis());

        byte[] data = new byte[size];
        new Random().nextBytes(data);
        Blob blob = store.createBlob(new ByteArrayInputStream(data));

        builder.child(child).setProperty("testBlob", blob);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return data;
    }

    private void addTestContentOnTheFly(NodeStore store, String child, long size, int seed) throws CommitFailedException, IOException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(child).setProperty("ts", System.currentTimeMillis());

        InputStream randomInputStream = newRandomInputStream(size, seed);
        Blob blob = store.createBlob(randomInputStream);

        builder.child(child).setProperty("testBlob", blob);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Before
    public void before() throws Exception {
        logger.info("Test begin: {}", testName.getMethodName());
    }

    @After
    public void after() {
        logger.info("Test end: {}", testName.getMethodName());
    }

    @Test
    public void testResilientSync() throws Exception {
        final int blobSize = 5 * MB;
        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        byte[] data = addTestContent(store, "server", blobSize);

        File spoolFolder = folder.newFolder();

        // run 1: unsuccessful
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB);
            StandbyClientSync cl = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, 4_000, false, spoolFolder)
        ) {
            serverSync.start();
            // no persisted head on primary
            // sync shouldn't be successful, but shouldn't throw exception either,
            // timeout too low for TarMK flush thread to kick-in
            cl.run();
            assertNotEquals(primary.getHead(), secondary.getHead());
        }

        // run 2: successful
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB);
            StandbyClientSync cl = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, 4_000, false, spoolFolder)
        ) {
            serverSync.start();
            // this time persisted head will be available on primary
            // waited at least 4s + 4s > 5s (TarMK flush thread run frequency)
            cl.run();
            assertEquals(primary.getHead(), secondary.getHead());
        }

        assertTrue(primary.getStats().getApproximateSize() < MB);
        assertTrue(secondary.getStats().getApproximateSize() < MB);

        PropertyState ps = secondary.getHead().getChildNode("root")
            .getChildNode("server").getProperty("testBlob");
        assertNotNull(ps);
        assertEquals(Type.BINARY.tag(), ps.getType().tag());
        Blob b = ps.getValue(Type.BINARY);
        assertEquals(blobSize, b.length());
        byte[] testData = new byte[blobSize];
        try (
            InputStream blobInputStream = b.getNewStream()
        ) {
            ByteStreams.readFully(blobInputStream, testData);
            assertArrayEquals(data, testData);
        }
    }

    @Test
    public void testSync() throws Exception {
        final int blobSize = 5 * MB;
        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        byte[] data = addTestContent(store, "server", blobSize);
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB);
            StandbyClientSync cl = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, getClientTimeout(), false, folder.newFolder())
        ) {
            serverSync.start();
            primary.flush();
            cl.run();
            assertEquals(primary.getHead(), secondary.getHead());
        }

        assertTrue(primary.getStats().getApproximateSize() < MB);
        assertTrue(secondary.getStats().getApproximateSize() < MB);

        PropertyState ps = secondary.getHead().getChildNode("root")
            .getChildNode("server").getProperty("testBlob");
        assertNotNull(ps);
        assertEquals(Type.BINARY.tag(), ps.getType().tag());
        Blob b = ps.getValue(Type.BINARY);
        assertEquals(blobSize, b.length());
        byte[] testData = new byte[blobSize];
        try (
            InputStream blobInputStream = b.getNewStream()
        ) {
            ByteStreams.readFully(blobInputStream, testData);
            assertArrayEquals(data, testData);
        }
    }

    /*
     * See OAK-5902.
     */
    @Test
    public void testSyncBigBlob() throws Exception {
        final long blobSize = GB;
        final int seed = 13;

        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        addTestContentOnTheFly(store, "server", blobSize, seed);

        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, 8 * MB);
            StandbyClientSync cl = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, 2 * 60 * 1000, false, folder.newFolder())
        ) {
            serverSync.start();
            primary.flush();
            cl.run();
            assertEquals(primary.getHead(), secondary.getHead());
        }

        assertTrue(primary.getStats().getApproximateSize() < MB);
        assertTrue(secondary.getStats().getApproximateSize() < MB);

        PropertyState ps = secondary.getHead().getChildNode("root")
            .getChildNode("server").getProperty("testBlob");
        assertNotNull(ps);
        assertEquals(Type.BINARY.tag(), ps.getType().tag());
        Blob b = ps.getValue(Type.BINARY);
        assertEquals(blobSize, b.length());

        try (
            InputStream randomInputStream = newRandomInputStream(blobSize, seed);
            InputStream blobInputStream = b.getNewStream()
        ) {
            assertTrue(IOUtils.contentEquals(randomInputStream, blobInputStream));
        }
    }

    /*
     * See OAK-4969.
     */
    @Test
    public void testSyncUpdatedBinaryProperty() throws Exception {
        final int blobSize = 5 * MB;

        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB);
            StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, getClientTimeout(), false, folder.newFolder())
        ) {
            serverSync.start();

            addTestContent(store, "server", blobSize);
            primary.flush();
            clientSync.run();
            assertEquals(primary.getHead(), secondary.getHead());

            addTestContent(store, "server", blobSize);
            primary.flush();
            clientSync.run();
            assertEquals(primary.getHead(), secondary.getHead());
        }
    }

    @Test
    public void testProxySkippedBytes() throws Exception {
        useProxy(100, 1, -1, false);
    }

    @Test
    public void testProxySkippedBytesIntermediateChange() throws Exception {
        useProxy(100, 1, -1, true);
    }

    @Test
    public void testProxyFlippedStartByte() throws Exception {
        useProxy(0, 0, 0, false);
    }

    @Test
    public void testProxyFlippedIntermediateByte() throws Exception {
        useProxy(0, 0, 150, false);
    }

    @Test
    public void testProxyFlippedIntermediateByte2() throws Exception {
        useProxy(0, 0, 150000, false);
    }

    @Test
    public void testProxyFlippedIntermediateByteChange() throws Exception {
        useProxy(0, 0, 150, true);
    }

    @Test
    public void testProxyFlippedIntermediateByteChange2() throws Exception {
        useProxy(0, 0, 150000, true);
    }

    private void useProxy(int skipPosition, int skipBytes, int flipPosition, boolean intermediateChange) throws Exception {
        int blobSize = 5 * MB;

        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        byte[] data = addTestContent(store, "server", blobSize);
        primary.flush();

        try (StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB)) {
            serverSync.start();

            File spoolFolder = folder.newFolder();

            try (
                NetworkErrorProxy ignored = new NetworkErrorProxy(proxyPort.getPort(), getServerHost(), serverPort.getPort(), flipPosition, skipPosition, skipBytes);
                StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), proxyPort.getPort(), secondary, false, getClientTimeout(), false, spoolFolder)
            ) {
                clientSync.run();
            }

            if (storesShouldBeDifferent()) {
                assertFalse("stores are equal", primary.getHead().equals(secondary.getHead()));
            }

            if (intermediateChange) {
                blobSize = 2 * MB;
                data = addTestContent(store, "server", blobSize);
                primary.flush();
            }

            try (StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, getClientTimeout(), false, spoolFolder)) {
                clientSync.run();
            }
        }

        assertEquals(primary.getHead(), secondary.getHead());

        assertTrue(primary.getStats().getApproximateSize() < MB);
        assertTrue(secondary.getStats().getApproximateSize() < MB);

        PropertyState ps = secondary.getHead()
            .getChildNode("root")
            .getChildNode("server")
            .getProperty("testBlob");
        assertNotNull(ps);
        assertEquals(Type.BINARY.tag(), ps.getType().tag());
        Blob b = ps.getValue(Type.BINARY);
        assertEquals(blobSize, b.length());
        byte[] testData = new byte[blobSize];
        try (InputStream blobInputStream = b.getNewStream()) {
            ByteStreams.readFully(blobInputStream, testData);
            assertArrayEquals(data, testData);
        }
    }
}
