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
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.createRegExpFilter;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.parseSegmentGraph;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.Graph;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentGraphTest {
    private final Set<UUID> segments = newHashSet(
        UUID.fromString("5be0c2ea-b6ba-4f80-acad-657a20f920b6"),
        UUID.fromString("fdaca71e-f71e-4f19-abf5-144e8c85f9e3"),
        UUID.fromString("53be3b93-87fa-487f-a2fc-7c17e639c231"),
        UUID.fromString("2eae0bc2-d3dd-4ba4-a765-70c38073437d"),
        UUID.fromString("ab61b8c9-222c-4119-a73b-5f61c0bc4741"),
        UUID.fromString("38c42dde-5928-4cc3-a483-37185d6971e4")
    );

    private final Map<UUID, Set<UUID>> references = ImmutableMap.<UUID, Set<UUID>>of(
        UUID.fromString("5be0c2ea-b6ba-4f80-acad-657a20f920b6"),
            newHashSet(UUID.fromString("2eae0bc2-d3dd-4ba4-a765-70c38073437d")),
        UUID.fromString("fdaca71e-f71e-4f19-abf5-144e8c85f9e3"),
            newHashSet(UUID.fromString("ab61b8c9-222c-4119-a73b-5f61c0bc4741")),
        UUID.fromString("2eae0bc2-d3dd-4ba4-a765-70c38073437d"),
            newHashSet(UUID.fromString("2fdaca71e-f71e-4f19-abf5-144e8c85f9e3"),
                       UUID.fromString("ab61b8c9-222c-4119-a73b-5f61c0bc4741"))
    );

    private final Set<UUID> filteredSegments = newHashSet(
        UUID.fromString("fdaca71e-f71e-4f19-abf5-144e8c85f9e3"),
        UUID.fromString("2eae0bc2-d3dd-4ba4-a765-70c38073437d"),
        UUID.fromString("ab61b8c9-222c-4119-a73b-5f61c0bc4741")
    );

    private final Map<UUID, Set<UUID>> filteredReferences = ImmutableMap.<UUID, Set<UUID>>of(
        UUID.fromString("fdaca71e-f71e-4f19-abf5-144e8c85f9e3"),
            newHashSet(UUID.fromString("ab61b8c9-222c-4119-a73b-5f61c0bc4741")),
        UUID.fromString("2eae0bc2-d3dd-4ba4-a765-70c38073437d"),
            newHashSet(UUID.fromString("2fdaca71e-f71e-4f19-abf5-144e8c85f9e3"),
                       UUID.fromString("ab61b8c9-222c-4119-a73b-5f61c0bc4741"))
    );

    private final Set<String> gcGenerations = newHashSet("0", "1");
    private final Map<String, Set<String>> gcReferences = ImmutableMap.of(
        "0", singleton("0"),
        "1", singleton("0")
    );

    @Rule
    public TemporaryFolder storeFolder = new TemporaryFolder(new File("target"));

    private File getStoreFolder() {
        return storeFolder.getRoot();
    }

    @Before
    public void setup() throws IOException {
        System.out.println(getStoreFolder());
        unzip(SegmentGraphTest.class.getResourceAsStream("file-store.zip"), getStoreFolder());
    }

    @Test
    public void testSegmentGraph() throws Exception {
        ReadOnlyStore store = FileStore.builder(getStoreFolder()).buildReadOnly();
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
        ReadOnlyStore store = FileStore.builder(getStoreFolder()).buildReadOnly();
        try {
            Predicate<UUID> filter = createRegExpFilter(".*testWriter.*", store.getTracker());
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
        ReadOnlyStore store = FileStore.builder(getStoreFolder()).buildReadOnly();
        try {
            Graph<String> gcGraph = SegmentGraph.parseGCGraph(store);
            assertEquals(gcGenerations, newHashSet(gcGraph.vertices()));
            Map<String, Set<String>> map = newHashMap();
            for (Entry<String, Multiset<String>> entry : gcGraph.edges()) {
                map.put(entry.getKey(), entry.getValue().elementSet());
            }
            assertEquals(gcReferences, map);
        } finally {
            store.close();
        }
    }

    private static void unzip(InputStream is, File target) throws IOException {
        ZipInputStream zis = new ZipInputStream(is);
        try {
            for (ZipEntry entry = zis.getNextEntry(); entry != null; entry = zis.getNextEntry()) {
                OutputStream out = new FileOutputStream(new File(target, entry.getName()));
                try {
                    IOUtils.copy(zis, out);
                } finally {
                    out.close();
                }
            }
        } finally {
            zis.close();
        }
    }
}
