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
package org.apache.jackrabbit.oak.plugins.segment.standby;

import com.google.common.io.ByteStreams;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.segment.NetworkErrorProxy;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.plugins.segment.standby.server.StandbyServer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

public class DataStoreTestBase extends TestBase {

    protected boolean storesCanBeEqual = false;

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    protected FileStore setupFileDataStore(File d, String path) throws IOException {
        FileDataStore fds = new FileDataStore();
        fds.setMinRecordLength(4092);
        fds.init(path);
        DataStoreBlobStore blobStore = new DataStoreBlobStore(fds);
        return new FileStore(blobStore, d, 1, false);
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

    @Test
    public void testSync() throws Exception {
        final int mb = 1 * 1024 * 1024;
        final int blobSize = 5 * mb;
        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = new SegmentNodeStore(primary);
        final StandbyServer server = new StandbyServer(getPort(), primary);
        server.start();
        byte[] data = addTestContent(store, "server", blobSize);

        StandbyClient cl = new StandbyClient("127.0.0.1", getPort(), secondary);
        cl.run();

        try {
            assertEquals(primary.getHead(), secondary.getHead());
        } finally {
            server.close();
            cl.close();
        }

        assertTrue(primary.size() < mb);
        assertTrue(secondary.size() < mb);

        PropertyState ps = secondary.getHead().getChildNode("root")
                .getChildNode("server").getProperty("testBlob");
        assertNotNull(ps);
        assertEquals(Type.BINARY.tag(), ps.getType().tag());
        Blob b = ps.getValue(Type.BINARY);
        assertEquals(blobSize, b.length());
        byte[] testData = new byte[blobSize];
        ByteStreams.readFully(b.getNewStream(), testData);
        assertArrayEquals(data, testData);
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
        int proxyPort = Integer.valueOf(System.getProperty("standby.proxy.port", "51913"));
        final int mb = 1 * 1024 * 1024;
        int blobSize = 5 * mb;
        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NetworkErrorProxy p = new NetworkErrorProxy(proxyPort, LOCALHOST, port);
        p.skipBytes(skipPosition, skipBytes);
        p.flipByte(flipPosition);
        p.run();

        NodeStore store = new SegmentNodeStore(primary);
        final StandbyServer server = new StandbyServer(getPort(), primary);
        server.start();
        byte[] data = addTestContent(store, "server", blobSize);

        StandbyClient cl = new StandbyClient("127.0.0.1", proxyPort, secondary);
        cl.run();

        try {
            if (skipBytes > 0 || flipPosition >= 0) {
                if (!this.storesCanBeEqual) {
                    assertFalse("stores are not expected to be equal", primary.getHead().equals(secondary.getHead()));
                }
                p.reset();
                if (intermediateChange) {
                    blobSize = 2 * mb;
                    data = addTestContent(store, "server", blobSize);
                }
                cl.run();
            }
            assertEquals(primary.getHead(), secondary.getHead());
        } finally {
            server.close();
            cl.close();
            p.close();
        }

        assertTrue(primary.size() < mb);
        assertTrue(secondary.size() < mb);

        PropertyState ps = secondary.getHead().getChildNode("root")
                .getChildNode("server").getProperty("testBlob");
        assertNotNull(ps);
        assertEquals(Type.BINARY.tag(), ps.getType().tag());
        Blob b = ps.getValue(Type.BINARY);
        assertEquals(blobSize, b.length());
        byte[] testData = new byte[blobSize];
        ByteStreams.readFully(b.getNewStream(), testData);
        assertArrayEquals(data, testData);
    }
}
