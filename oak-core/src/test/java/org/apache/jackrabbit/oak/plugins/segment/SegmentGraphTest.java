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


import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.io.File.createTempFile;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.parseSegmentGraph;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.Graph;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SegmentGraphTest {
    private final Set<UUID> segments = newHashSet();
    private final Map<UUID, Set<UUID>> references = newHashMap();

    private final Set<Integer> gcGenerations = newHashSet();
    private final Map<Integer, Set<Integer>> gcReferences = newHashMap();

    private File storeDir;

    private void addSegment(SegmentNodeState node) {
        segments.add(node.getRecordId().asUUID());
    }

    private void addReference(SegmentNodeState from, SegmentNodeState to) {
        addReference(references, from, to);
    }

    private static void addReference(Map<UUID, Set<UUID>> map, SegmentNodeState from, SegmentNodeState to) {
        UUID fromUUID = from.getRecordId().asUUID();
        UUID toUUID = to.getRecordId().asUUID();
        Set<UUID> tos = map.get(fromUUID);
        if (tos == null) {
            tos = newHashSet();
            map.put(fromUUID, tos);
        }
        tos.add(toUUID);
    }

    @Before
    public void setup() throws IOException {
        storeDir = createTempFile(SegmentGraph.class.getSimpleName(), null);
        storeDir.delete();
        storeDir.mkdir();

        FileStore store = FileStore.newFileStore(storeDir).create();
        CompactionStrategy strategy = new CompactionStrategy(false, false, CleanupType.CLEAN_NONE, 0, (byte) 0) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
                return setHead.call();
            }
        };
        strategy.setPersistCompactionMap(false);
        store.setCompactionStrategy(strategy);

        addSegment(store.getHead());

        SegmentWriter writer = store.createSegmentWriter("testWriter");
        SegmentNodeState node1 = writer.writeNode(EMPTY_NODE);
        addSegment(node1);
        writer.flush();

        SegmentNodeState node2 = writer.writeNode(EMPTY_NODE);
        addSegment(node2);
        addReference(node2, node1);  // Through the respective templates, which are de-duplicated
        writer.flush();

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("foo", node1);
        builder.setChildNode("bar", node2);

        SegmentNodeState node3 = writer.writeNode(builder.getNodeState());
        addSegment(node3);
        addReference(node3, node1);
        addReference(node3, node2);
        writer.flush();

        store.setHead(store.getHead(), node3);

        store.compact();
        addSegment(store.getHead());
        builder = node3.builder();
        builder.setProperty("p", 42);
        SegmentNodeState node4 = writer.writeNode(builder.getNodeState());
        addReference(node4, node3);
        addSegment(node4);
        store.setHead(store.getHead(), node4);

        store.close();

        gcGenerations.add(0);
        gcGenerations.add(1);
        gcReferences.put(0, singleton(0));
        gcReferences.put(1, singleton(0));
    }

    @After
    public void tearDown() {
        storeDir.delete();
    }

    @Test
    public void testSegmentGraph() throws IOException {
        ReadOnlyStore store = new ReadOnlyStore(storeDir);
        try {
            Graph<UUID> segmentGraph = parseSegmentGraph(store);
            assertEquals(segments, segmentGraph.vertices);
            assertEquals(references, segmentGraph.edges);
        } finally {
            store.close();
        }
    }

    @Test
    public void testGCGraph() throws IOException {
        ReadOnlyStore store = new ReadOnlyStore(storeDir);
        try {
            Graph<Integer> gcGraph = SegmentGraph.parseGCGraph(store);
            assertEquals(gcGenerations, gcGraph.vertices);
            assertEquals(gcReferences, gcGraph.edges);
        } finally {
            store.close();
        }
    }
}
