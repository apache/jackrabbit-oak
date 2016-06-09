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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_10;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentVersionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    private File directory;

    @Test
    public void latestVersion() {
        assertEquals(V_11, LATEST_VERSION);
    }

    @Test
    public void compareOldRevision() throws Exception {
        FileStore fileStoreV10 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).withSegmentVersion(V_10).build();
        try {
            NodeState content = addTestContent(fileStoreV10, "content").getChildNode("content");
            assertVersion(content, SegmentVersion.V_10);
            NodeBuilder builder = content.builder();
            builder.setChildNode("foo");
            content.compareAgainstBaseState(builder.getNodeState(), new NodeStateDiff() {
                @Override
                public boolean propertyAdded(PropertyState after) {
                    fail();
                    return false;
                }

                @Override
                public boolean propertyChanged(PropertyState before, PropertyState after) {
                    fail();
                    return false;
                }

                @Override
                public boolean propertyDeleted(PropertyState before) {
                    fail();
                    return false;
                }

                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    fail();
                    return false;
                }

                @Override
                public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                    fail();
                    return false;
                }

                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    assertEquals("foo", name);
                    return false;
                }
            });
        } finally {
            fileStoreV10.close();
        }
    }

    @Test
    public void readOldVersions() throws Exception {
        FileStore fileStoreV10 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).withSegmentVersion(V_10).build();
        try {
            NodeState content = addTestContent(fileStoreV10, "content");
            assertVersion(content, SegmentVersion.V_10);
        } finally {
            fileStoreV10.close();
        }

        FileStore fileStoreV11 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).build();
        try {
            verifyContent(fileStoreV11, "content");
        } finally {
            fileStoreV11.close();
        }
    }

    @Test
    public void mixedVersions() throws IOException, CommitFailedException {
        FileStore fileStoreV10 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).withSegmentVersion(V_10).build();
        try {
            NodeState content10 = addTestContent(fileStoreV10, "content10");
            assertVersion(content10, SegmentVersion.V_10);
        } finally {
            fileStoreV10.close();
        }

        FileStore fileStoreV11 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).build();
        try {
            NodeState content11 = addTestContent(fileStoreV11, "content11");
            assertVersion(content11, V_11);
            verifyContent(fileStoreV11, "content10");
            verifyContent(fileStoreV11, "content11");
        } finally {
            fileStoreV11.close();
        }
    }

    @Test
    public void migrate() throws IOException, CommitFailedException {
        FileStore fileStoreV10 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).withSegmentVersion(V_10).build();
        try {
            addTestContent(fileStoreV10, "content10");
        } finally {
            fileStoreV10.close();
        }

        FileStore fileStoreV11 = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).build();
        try {
            fileStoreV11.setCompactionStrategy(new CompactionStrategy(false, false,
                    CLEAN_NONE, 0, (byte) 0) {
                @Override
                public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
                    return setHead.call();
                }
            });
            checkAllVersions(fileStoreV11.getHead(), SegmentVersion.V_10);
            fileStoreV11.compact();
            checkAllVersions(fileStoreV11.getHead(), V_11);
        } finally {
            fileStoreV11.close();
        }
    }

    private static void checkAllVersions(SegmentNodeState head, SegmentVersion version) {
        assertVersion(head, version);
        for (ChildNodeEntry childNodeEntry : head.getChildNodeEntries()) {
            checkAllVersions((SegmentNodeState) childNodeEntry.getNodeState(), version);
        }
    }

    private static void assertVersion(NodeState node, SegmentVersion version) {
        assertTrue(node instanceof SegmentNodeState);
        assertEquals(version, ((SegmentNodeState) node).getSegment().getSegmentVersion());
    }

    @SuppressWarnings("deprecation")
    private static NodeState addTestContent(FileStore fs, String nodeName)
            throws CommitFailedException {
        NodeStore store = SegmentNodeStore.builder(fs).build();
        NodeBuilder builder = store.getRoot().builder();

        NodeBuilder content = builder.child(nodeName);
        content.setProperty("a", 1);
        content.setProperty("aM", ImmutableList.of(1L, 2L, 3L, 4L), LONGS);

        content.setProperty("b", "azerty");
        content.setProperty("bM",
                ImmutableList.of("a", "z", "e", "r", "t", "y"), STRINGS);

        // add blobs?

        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void verifyContent(FileStore fs, String nodeName) {
        NodeStore store = SegmentNodeStore.builder(fs).build();
        SegmentNodeState content = (SegmentNodeState) store.getRoot()
                .getChildNode(nodeName);

        assertEquals(new Long(1), content.getProperty("a").getValue(LONG));
        assertEquals(ImmutableList.of(1L, 2L, 3L, 4L),
                Lists.newArrayList(content.getProperty("aM").getValue(LONGS)));

        assertEquals("azerty", content.getProperty("b").getValue(STRING));
        assertEquals("azerty", content.getString("b"));

        assertEquals(ImmutableList.of("a", "z", "e", "r", "t", "y"),
                Lists.newArrayList(content.getProperty("bM").getValue(STRINGS)));
        assertEquals(ImmutableList.of("a", "z", "e", "r", "t", "y"),
                Lists.newArrayList(content.getStrings("bM")));
    }
}
