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

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test asserts that adding many ({@link ManyChildNodesIT#NODE_COUNT}) child nodes
 * consumes a constant amount of memory (See OAK-4949). It should be possible to add
 * 2M child nodes with a 512M heap.
 *
 *<p>The test is <b>disabled</b> by default, to run it set {@code -Dtest=ManyChildNodesIT} </p>
 */
public class ManyChildNodesIT {
    private static final int NODE_COUNT = getInteger("many-child-node-it.node-count", 2000000);

    /** Only run if explicitly asked to via -Dtest=ManyChildNodesIT */
    private static final boolean ENABLED =
            ManyChildNodesIT.class.getSimpleName().equals(getProperty("test"));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws Exception {
        assumeTrue(ENABLED);
    }

    @Nonnull
    private FileStore createFileStore() throws InvalidFileStoreVersionException, IOException {
        return fileStoreBuilder(folder.getRoot())
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withStringDeduplicationCacheSize(0)
                .withTemplateDeduplicationCacheSize(0)
                .withNodeDeduplicationCacheSize(1)
                .withGCOptions(defaultGCOptions().setOffline())
                .build();
    }

    @Test
    public void manyChildNodesOnRoot()
    throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        try (FileStore fileStore = createFileStore()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();
            addManyNodes(builder);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    @Test
    public void manyChildNodes()
    throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        try (FileStore fileStore = createFileStore()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder root = nodeStore.getRoot().builder();
            addManyNodes(root.setChildNode("a").setChildNode("b"));
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    /**
     * Offline compaction should be able to deal with many child nodes in constant memory.
     */
    @Test
    public void manyChildNodesOC() throws Exception {
        try (FileStore fileStore = createFileStore()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            nodeStore.checkpoint(Long.MAX_VALUE);
            NodeBuilder builder = nodeStore.getRoot().builder();
            addManyNodes(builder);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            NodeState uncompactedRoot = nodeStore.getRoot();
            assertTrue(fileStore.compactFull());
            NodeState compactedRoot = nodeStore.getRoot();
            assertTrue(uncompactedRoot != compactedRoot);
            assertEquals(uncompactedRoot, compactedRoot);
        }
    }

    private static void addManyNodes(NodeBuilder builder) {
        for (int k = 0; k < NODE_COUNT; k++) {
            if (k % 10000 == 0) System.out.println(k);
            builder.setChildNode("c-" + k);
        }
    }

}
