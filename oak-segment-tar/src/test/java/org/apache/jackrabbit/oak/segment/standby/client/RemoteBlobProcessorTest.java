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

package org.apache.jackrabbit.oak.segment.standby.client;

import java.io.File;

import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.segment.test.TemporaryBlobStore;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class RemoteBlobProcessorTest {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryBlobStore blobStore = new TemporaryBlobStore(folder) {

        @Override
        protected void configureDataStore(FileDataStore dataStore) {
            dataStore.setMinRecordLength(SegmentTestConstants.MEDIUM_LIMIT);
        }

    };

    private TemporaryFileStore fileStore = new TemporaryFileStore(folder, blobStore, false);

    @Rule
    public RuleChain rules = RuleChain.outerRule(folder)
        .around(blobStore)
        .around(fileStore);

    /**
     * The test relies on a particular interaction between the BlobStore and the
     * FileStore.
     *
     * <ul>
     * <li>The FileStore passes every binary whose length is grater than or
     * equal to MEDIUM_LIMIT bytes to the BlobStore.</li>
     *
     * <li>The BlobStore creates in-memory IDs (where the binary content is
     * embedded in the ID itself) if the binary's length is smaller than or
     * equal to minRecordLength bytes.</li>
     *
     * <li>It follows that if minRecordLength is set to at least MEDIUM_LIMIT,
     * the FileStore will delegate the handling of the binary to the BlobStore,
     * which will create an in-memory ID embedding the content of the
     * binary.</li>
     * </ul>
     *
     * <p>
     * With this configuration in place, a binary long exactly MEDIUM_LIMIT
     * bytes will be passed from the File Store to the Blob Store, which will
     * create an in-memory ID. The File Store will persist the in-memory ID,
     * which contains the binary data itself. This binary should never be
     * downloaded.
     */
    @Test
    public void inMemoryBinaryShouldNotBeDownloaded() throws Exception {
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore.fileStore()).build();

        NodeBuilder root = store.getRoot().builder();
        root.setProperty("b", root.createBlob(new NullInputStream(SegmentTestConstants.MEDIUM_LIMIT)));
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        RemoteBlobProcessor processor = new RemoteBlobProcessor(blobStore.blobStore(), blobId -> {
            Assert.fail("In-memory binaries should not be downloaded");
            return null;
        });

        processor.processBinary(store.getRoot().getProperty("b").getValue(Type.BINARY));
    }

    @Test
    public void inlineBinaryShouldNotBeDownloaded() throws Exception {
        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore.fileStore()).build();

        NodeBuilder root = store.getRoot().builder();
        root.setProperty("b", root.createBlob(new NullInputStream(SegmentTestConstants.MEDIUM_LIMIT - 1)));
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        RemoteBlobProcessor processor = new RemoteBlobProcessor(blobStore.blobStore(), blobId -> {
            Assert.fail("Inline binaries should not be downloaded");
            return null;
        });

        processor.processBinary(store.getRoot().getProperty("b").getValue(Type.BINARY));
    }

}
