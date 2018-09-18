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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Random;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class StandbyTestIT extends TestBase {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, false);

    private TemporaryFileStore clientFileStore = new TemporaryFileStore(folder, true);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
            .around(serverFileStore)
            .around(clientFileStore);

    @Test
    public void testSync() throws Exception {
        int blobSize = 5 * MB;
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB);
            StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, getClientTimeout(), false, folder.newFolder())
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, 150);
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

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

    /**
     * OAK-2430
     */
    @Test
    public void testSyncLoop() throws Exception {
        final int blobSize = 25 * 1024;
        final int dataNodes = 5000;

        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), primary, MB);
            StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, getClientTimeout(), false, folder.newFolder())
        ) {
            serverSync.start();
            byte[] data = addTestContent(store, "server", blobSize, dataNodes);
            primary.flush();

            for (int i = 0; i < 5; i++) {
                String cp = store.checkpoint(Long.MAX_VALUE);
                primary.flush();
                clientSync.run();
                assertEquals(primary.getHead(), secondary.getHead());
                assertTrue(store.release(cp));
                clientSync.cleanup();
                assertTrue(secondary.getStats().getApproximateSize() > blobSize);
            }

            assertTrue(primary.getStats().getApproximateSize() > blobSize);
            assertTrue(secondary.getStats().getApproximateSize() > blobSize);

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

    private static byte[] addTestContent(NodeStore store, String child, int size, int dataNodes) throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder content = builder.child(child);
        content.setProperty("ts", System.currentTimeMillis());

        byte[] data = new byte[size];
        new Random().nextBytes(data);
        Blob blob = store.createBlob(new ByteArrayInputStream(data));
        content.setProperty("testBlob", blob);

        for (int i = 0; i < dataNodes; i++) {
            NodeBuilder c = content.child("c" + i);
            for (int j = 0; j < 1000; j++) {
                c.setProperty("p" + i, "v" + i);
            }
        }

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return data;
    }

}
