/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.addTestContent;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.assertSameRecord;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.assertSameStableId;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.checkGeneration;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.createBlob;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.createCompactor;
import static org.apache.jackrabbit.oak.segment.CheckpointCompactorTestUtils.getCheckpoint;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.test.TemporaryBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class CheckpointCompactorExternalBlobTest {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryBlobStore tempoararyBlobStore = new TemporaryBlobStore(folder);

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    private CheckpointCompactor compactor;

    private GCGeneration compactedGeneration;

    @Rule
    public RuleChain rules = RuleChain.outerRule(folder)
        .around(tempoararyBlobStore);

    public void setup(boolean withBlobStore) throws IOException, InvalidFileStoreVersionException {
        BlobStore blobStore = tempoararyBlobStore.blobStore();
        FileStoreBuilder fileStoreBuilder = fileStoreBuilder(folder.getRoot());

        if (withBlobStore) {
            fileStoreBuilder = fileStoreBuilder.withBlobStore(blobStore);
        }

        fileStore = fileStoreBuilder.build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        compactedGeneration = newGCGeneration(1,1, true);
        compactor = createCompactor(fileStore, compactedGeneration);
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        setup(true);

        // add two blobs which will be persisted in the blob store
        addTestContent("cp1", nodeStore, SegmentTestConstants.MEDIUM_LIMIT);
        String cp1 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp2", nodeStore, SegmentTestConstants.MEDIUM_LIMIT);
        String cp2 = nodeStore.checkpoint(DAYS.toMillis(1));

        // update the two blobs from the blob store
        updateTestContent("cp1", nodeStore);
        String cp3 = nodeStore.checkpoint(DAYS.toMillis(1));
        updateTestContent("cp2", nodeStore);
        String cp4 = nodeStore.checkpoint(DAYS.toMillis(1));
        fileStore.close();

        // no blob store configured
        setup(false);

        // this time the updated blob will be stored in the file store
        updateTestContent("cp2", nodeStore);
        String cp5 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        SegmentNodeState compacted1 = compactor.compact(EMPTY_NODE, uncompacted1, EMPTY_NODE, Canceller.newCanceller());

        assertNotNull(compacted1);
        assertFalse(uncompacted1 == compacted1);
        checkGeneration(compacted1, compactedGeneration);

        assertSameStableId(uncompacted1, compacted1);
        assertSameStableId(getCheckpoint(uncompacted1, cp1), getCheckpoint(compacted1, cp1));
        assertSameStableId(getCheckpoint(uncompacted1, cp2), getCheckpoint(compacted1, cp2));
        assertSameStableId(getCheckpoint(uncompacted1, cp3), getCheckpoint(compacted1, cp3));
        assertSameStableId(getCheckpoint(uncompacted1, cp4), getCheckpoint(compacted1, cp4));
        assertSameStableId(getCheckpoint(uncompacted1, cp5), getCheckpoint(compacted1, cp5));
        assertSameRecord(getCheckpoint(compacted1, cp5), compacted1.getChildNode("root"));
    }

    private static void updateTestContent(@NotNull String parent, @NotNull NodeStore nodeStore)
            throws CommitFailedException, IOException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder parentBuilder = rootBuilder.child(parent);
        parentBuilder.child("b").setProperty("bin", createBlob(nodeStore, SegmentTestConstants.MEDIUM_LIMIT));
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}