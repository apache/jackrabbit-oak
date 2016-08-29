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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.segment.SegmentGraph.createRegExpFilter;
import static org.apache.jackrabbit.oak.segment.SegmentGraph.parseSegmentGraph;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import org.apache.jackrabbit.oak.segment.SegmentGraph.Graph;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentGraphTest {
    private final Set<UUID> segments = newHashSet();
    private final Map<UUID, Set<UUID>> references = newHashMap();
    private final Set<UUID> filteredSegments = newHashSet();
    private final Map<UUID, Set<UUID>> filteredReferences = newHashMap();

    @Rule
    public TemporaryFolder storeFolder = new TemporaryFolder(new File("target"));

    private File getStoreFolder() {
         return storeFolder.getRoot();
    }

    @Before
    public void setup() throws Exception {
        FileStore store = fileStoreBuilder(getStoreFolder()).build();
        try {
            SegmentNodeState root = store.getHead();
            segments.add(getSegmentId(root));

            SegmentWriter w1 = segmentWriterBuilder("writer1").build(store);
            SegmentWriter w2 = segmentWriterBuilder("writer2").build(store);
            SegmentWriter w3 = segmentWriterBuilder("writer3").build(store);

            SegmentPropertyState p1 = w1.writeProperty(createProperty("p1", "v1"));
            segments.add(getSegmentId(p1));

            SegmentPropertyState p2 = w2.writeProperty(createProperty("p2", "v2"));
            segments.add(getSegmentId(p2));
            filteredSegments.add(getSegmentId(p2));

            SegmentPropertyState p3 = w3.writeProperty(createProperty("p3", "v3"));
            segments.add(getSegmentId(p3));
            filteredSegments.add(getSegmentId(p3));

            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setProperty(p1);
            builder.setProperty(p2);
            builder.setProperty(p3);

            SegmentNodeState n3 = w3.writeNode(builder.getNodeState());
            segments.add(getSegmentId(n3));
            filteredSegments.add(getSegmentId(n3));
            addReference(references, getSegmentId(n3), getSegmentId(p1));
            addReference(references, getSegmentId(n3), getSegmentId(p2));
            addReference(filteredReferences, getSegmentId(n3), getSegmentId(p2));

            // Cyclic reference
            SegmentNodeState n1 = w1.writeNode(builder.getNodeState());
            addReference(references, getSegmentId(n1), getSegmentId(p2));
            addReference(references, getSegmentId(n1), getSegmentId(p3));

            store.getRevisions().setHead(root.getRecordId(), n3.getRecordId());

            w1.flush();
            w2.flush();
            w3.flush();
        } finally {
            store.close();
        }
    }

    private static void addReference(Map<UUID, Set<UUID>> references, UUID from, UUID to) {
        Set<UUID> tos = references.get(from);
        if (tos == null) {
            tos = newHashSet();
            references.put(from, tos);
        }
        tos.add(to);
    }

    private static UUID getSegmentId(SegmentPropertyState p1) {return p1.getSegment().getSegmentId().asUUID();}

    private static UUID getSegmentId(SegmentNodeState root) {return root.getSegment().getSegmentId().asUUID();}

    @Test
    public void testSegmentGraph() throws Exception {
        ReadOnlyStore store = fileStoreBuilder(getStoreFolder()).buildReadOnly();
        try {
            Graph<UUID> segmentGraph = parseSegmentGraph(store, Predicates.<UUID>alwaysTrue());
            assertEquals(segments, newHashSet(segmentGraph.vertices()));
            Map<UUID, Set<UUID>> map = newHashMap();
            for (Entry<UUID, Multiset<UUID>> entry : segmentGraph.edges()) {
                map.put(entry.getKey(), entry.getValue().elementSet());
            }
            assertEquals(references, map);
        } finally {
            store.close();
        }
    }

    @Test
    public void testSegmentGraphWithFilter() throws Exception {
        ReadOnlyStore store = fileStoreBuilder(getStoreFolder()).buildReadOnly();
        try {
            Predicate<UUID> filter = createRegExpFilter(".*(writer2|writer3).*", store);
            Graph<UUID> segmentGraph = parseSegmentGraph(store, filter);
            assertEquals(filteredSegments, newHashSet(segmentGraph.vertices()));
            Map<UUID, Set<UUID>> map = newHashMap();
            for (Entry<UUID, Multiset<UUID>> entry : segmentGraph.edges()) {
                map.put(entry.getKey(), entry.getValue().elementSet());
            }
            assertEquals(filteredReferences, map);
        } finally {
            store.close();
        }
    }

    @Test
    public void testGCGraph() throws Exception {
        // TODO Improve test coverage to non trivial cases with more than a single generation
        // This is quite tricky as there is no easy way to construct a file store with
        // a segment graphs having edges between generations (OAK-3348)
        ReadOnlyStore store = fileStoreBuilder(getStoreFolder()).buildReadOnly();
        try {
            Graph<String> gcGraph = SegmentGraph.parseGCGraph(store);
            assertEquals(ImmutableSet.of("0"), newHashSet(gcGraph.vertices()));
            Map<String, Set<String>> map = newHashMap();
            for (Entry<String, Multiset<String>> entry : gcGraph.edges()) {
                map.put(entry.getKey(), entry.getValue().elementSet());
            }
            assertEquals(ImmutableMap.of(
                "0", singleton("0")
            ), map);
        } finally {
            store.close();
        }
    }

}
