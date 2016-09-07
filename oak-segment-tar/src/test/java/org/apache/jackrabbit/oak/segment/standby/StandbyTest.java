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
import java.io.IOException;
import java.util.Random;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class StandbyTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    public void testSync() throws Exception {
        final int mb = 1 * 1024 * 1024;
        final int blobSize = 5 * mb;
        FileStore primary = getPrimary();
        FileStore secondary = getSecondary();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        final StandbyServer server = new StandbyServer(port, primary);
        server.start();
        byte[] data = addTestContent(store, "server", blobSize, 150);
        primary.flush();

        StandbyClient cl = newStandbyClient(secondary);
        cl.run();

        try {
            assertEquals(primary.getHead(), secondary.getHead());
        } finally {
            server.close();
            cl.close();
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

    private static byte[] addTestContent(NodeStore store, String child, int size, int dataNodes)
            throws CommitFailedException, IOException {
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
