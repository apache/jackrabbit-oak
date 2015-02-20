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

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentVersionTest {

    private static final Logger log = LoggerFactory
            .getLogger(SegmentVersionTest.class);

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile("VersionTest", "dir",
                new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void cleanDir() {
        try {
            deleteDirectory(directory);
        } catch (IOException e) {
            log.error("Error cleaning directory", e);
        }
    }

    @Test
    public void compareOldRevision() throws Exception {
        FileStore fileStoreV10 = new FileStore(directory, 1) {
            @SuppressWarnings("deprecation")
            @Override
            public SegmentVersion getVersion() {
                return SegmentVersion.V_10;
            }
        };
        try {
            NodeState content = addTestContent(fileStoreV10).getChildNode("content");
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
        FileStore fileStoreV10 = new FileStore(directory, 1) {
            @SuppressWarnings("deprecation")
            @Override
            public SegmentVersion getVersion() {
                return SegmentVersion.V_10;
            }
        };
        try {
            addTestContent(fileStoreV10);
        } finally {
            fileStoreV10.close();
        }

        FileStore fileStoreV11 = new FileStore(directory, 1);
        try {
            verifyContent(fileStoreV11);
        } finally {
            fileStoreV11.close();
        }

    }

    @SuppressWarnings("deprecation")
    private static NodeState addTestContent(FileStore fs)
            throws CommitFailedException {
        NodeStore store = new SegmentNodeStore(fs);
        NodeBuilder builder = store.getRoot().builder();

        NodeBuilder content = builder.child("content");
        content.setProperty("a", 1);
        content.setProperty("aM", ImmutableList.of(1L, 2L, 3L, 4L), LONGS);

        content.setProperty("b", "azerty");
        content.setProperty("bM",
                ImmutableList.of("a", "z", "e", "r", "t", "y"), STRINGS);

        // add blobs?

        NodeState root = store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertEquals(SegmentVersion.V_10, fs.getHead().getSegment()
                .getSegmentVersion());
        return root;
    }

    private static void verifyContent(FileStore fs)
            throws CommitFailedException {
        NodeStore store = new SegmentNodeStore(fs);
        SegmentNodeState content = (SegmentNodeState) store.getRoot()
                .getChildNode("content");

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
