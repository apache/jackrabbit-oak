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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.CIHelper;
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

public abstract class DataStoreTestBase extends TestBase {
    static final long GB = 1024 * 1024 * 1024;

    private NetworkErrorProxy proxy;

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public TemporaryPort proxyPort = new TemporaryPort();

    abstract FileStore getPrimary();

    abstract FileStore getSecondary();

    abstract boolean storesShouldBeEqual();

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
    
    protected void addTestContentOnTheFly(NodeStore store, String child, long size, int seed) throws CommitFailedException, IOException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child(child).setProperty("ts", System.currentTimeMillis());

        InputStream randomInputStream = newRandomInputStream(size, seed);
        Blob blob = store.createBlob(randomInputStream);

        builder.child(child).setProperty("testBlob", blob);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Before
    public void before() {
        proxy = new NetworkErrorProxy(proxyPort.getPort(), getServerHost(), serverPort.getPort());
    }

    @After
    public void after() {
        proxy.close();
    }

    @Test
    public void testSync() throws Exception {
        final int blobSize = 5 * MB;
        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        byte[] data = addTestContent(store, "server", blobSize);
        try (
                StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, 1 * MB);
                StandbyClientSync cl = newStandbyClientSync(secondary, serverPort.getPort())
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
        assumeFalse(CIHelper.jenkins());  // FIXME OAK-6678: fails on Jenkins
        
        final long blobSize = (long) (1 * GB);
        final int seed = 13;

        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        addTestContentOnTheFly(store, "server", blobSize, seed);

        try (
                StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, 8 * MB);
                StandbyClientSync cl = newStandbyClientSync(secondary, serverPort.getPort(), 60_000)
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
                StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, 1 * MB);
                StandbyClientSync clientSync = newStandbyClientSync(secondary, serverPort.getPort())
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
        try (
                StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, 1 * MB);
                StandbyClientSync clientSync = newStandbyClientSync(secondary, proxyPort.getPort())
        ) {
            proxy.skipBytes(skipPosition, skipBytes);
            proxy.flipByte(flipPosition);
            proxy.connect();

            serverSync.start();
            primary.flush();

            clientSync.run();

            if (skipBytes > 0 || flipPosition >= 0) {
                if (!storesShouldBeEqual()) {
                    assertFalse("stores are not expected to be equal", primary.getHead().equals(secondary.getHead()));
                }
                proxy.reset();
                if (intermediateChange) {
                    blobSize = 2 * MB;
                    data = addTestContent(store, "server", blobSize);
                    primary.flush();
                }
                clientSync.run();
            }
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
}
