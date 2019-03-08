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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.TemporaryBlobStore;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class StandbySegmentBlobTestIT extends TestBase {

    // `BLOB_SIZE` has to be chosen in such a way that is both:
    //
    // - Less than or equal to `minRecordLength` in the Blob Store. This is
    //   necessary in order to create a sufficiently long blob ID.
    // - Greater than or equal to `Segment.BLOB_ID_SMALL_LIMIT` in order to save
    //   the blob ID as a large, external blob ID.
    // - Greater than or equal to `Segment.MEDIUM_LIMIT`, otherwise the content
    //   of the binary will be written as a mere value record instead of a
    //   binary ID referencing an external binary.
    //
    // Since `Segment.MEDIUM_LIMIT` is the largest of the constants above, it is
    // sufficient to set `BLOB_SIZE` to `Segment.MEDIUM_LIMIT`.

    private static final int BLOB_SIZE = 2 * SegmentTestConstants.MEDIUM_LIMIT;

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryBlobStore blobStore = new TemporaryBlobStore(folder) {

        @Override
        protected void configureDataStore(FileDataStore dataStore) {
            dataStore.setMinRecordLength(BLOB_SIZE);
        }

    };

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, blobStore, false);

    private TemporaryFileStore clientFileStore = new TemporaryFileStore(folder, blobStore, true);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
            .around(blobStore)
            .around(serverFileStore)
            .around(clientFileStore);

    @Test
    public void testSyncWithLongBlobId() throws Exception {
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(primary).build();
        try (
            StandbyServerSync serverSync = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(MB)
                .build();
            StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), secondary, false, getClientTimeout(), false, folder.newFolder())
        ) {
            serverSync.start();
            addTestContent(store, "server");
            primary.flush();

            clientSync.run();

            assertEquals(primary.getHead(), secondary.getHead());

            PropertyState ps = secondary.getHead().getChildNode("root")
                    .getChildNode("server").getProperty("testBlob");
            assertNotNull(ps);
            assertEquals(Type.BINARY.tag(), ps.getType().tag());
            Blob b = ps.getValue(Type.BINARY);
            assertEquals(BLOB_SIZE, b.length());
        }
    }

    private static void addTestContent(NodeStore store, String child) throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder content = builder.child(child);
        content.setProperty("ts", System.currentTimeMillis());

        Blob blob = store.createBlob(new NullInputStream(BLOB_SIZE));
        content.setProperty("testBlob", blob);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
