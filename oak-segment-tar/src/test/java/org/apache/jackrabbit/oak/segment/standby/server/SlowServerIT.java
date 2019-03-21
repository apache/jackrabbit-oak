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

package org.apache.jackrabbit.oak.segment.standby.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
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

public class SlowServerIT {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryBlobStore serverBlobStore = new TemporaryBlobStore(folder);

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, serverBlobStore, false);

    private TemporaryBlobStore clientBlobStore = new TemporaryBlobStore(folder);

    private TemporaryFileStore clientFileStore = new TemporaryFileStore(folder, clientBlobStore, true);

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
        .around(serverBlobStore)
        .around(serverFileStore)
        .around(clientBlobStore)
        .around(clientFileStore);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Test
    public void testSyncFailingDueToTooShortTimeout() throws Exception {
        FileStore primary = serverFileStore.fileStore();
        FileStore secondary = clientFileStore.fileStore();

        // Add a node on the primary that references a 5MB binary.

        createTestContent(primary);

        // Configure a StandbyBlobReader that behaves like the default one, but
        // adds a 2s delay every time a binary is fetched from the Data Store.

        StandbyBlobReader blobReader = newDelayedBlobReader(2, TimeUnit.SECONDS, new DefaultStandbyBlobReader(primary.getBlobStore()));

        try (

            // The primary uses the delayed StandbyBlobReader and is configured
            // to transfer binaries in chunks of 1MB.

            StandbyServerSync server = StandbyServerSync.builder()
                .withPort(serverPort.getPort())
                .withFileStore(primary)
                .withBlobChunkSize(1024 * 1024)
                .withStandbyBlobReader(blobReader)
                .build();

            // The client expects the server to respond withing 1s. When the
            // binary is requested, the delay on the server guarantees that the
            // timeout on the client will expire.

            StandbyClientSync client = new StandbyClientSync("localhost", serverPort.getPort(), secondary, false, 1000, false, folder.newFolder())
        ) {
            server.start();
            client.run();
            assertNotEquals(primary.getHead(), secondary.getHead());
            assertEquals(1, client.getFailedRequests());
        }
    }

    private void createTestContent(FileStore fileStore) throws CommitFailedException, IOException {
        NodeStore store = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder builder = store.getRoot().builder();

        Blob blob = store.createBlob(new NullInputStream(5 * 1024 * 102));
        builder.child("n").setProperty("data", blob);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        fileStore.flush();
    }

    private StandbyBlobReader newDelayedBlobReader(int delay, TimeUnit timeUnit, StandbyBlobReader wrapped) {
        return new StandbyBlobReader() {

            @Override
            public InputStream readBlob(String blobId) {
                try {
                    Thread.sleep(timeUnit.toMillis(delay));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return wrapped.readBlob(blobId);
            }

            @Override
            public long getBlobLength(String blobId) {
                return wrapped.getBlobLength(blobId);
            }

        };
    }

}
