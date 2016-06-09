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

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.segment.CompactionMap.sum;
import static org.apache.jackrabbit.oak.plugins.segment.TestUtils.newRecordId;
import static org.apache.jackrabbit.oak.plugins.segment.TestUtils.randomRecordIdMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CompactionMapTest {

    private final Random rnd = new Random();

    private final boolean usePersistedMap;

    private FileStore store;

    private Map<RecordId, RecordId> referenceMap1;
    private Map<RecordId, RecordId> referenceMap2;
    private Map<RecordId, RecordId> referenceMap3;
    private Map<RecordId, RecordId> referenceMap = newHashMap();

    private PartialCompactionMap compactionMap1;
    private PartialCompactionMap compactionMap2;
    private PartialCompactionMap compactionMap3;
    private CompactionMap compactionMap;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Parameterized.Parameters
    public static List<Boolean[]> fixtures() {
        return ImmutableList.of(new Boolean[] {true}, new Boolean[] {false});
    }

    @Before
    public void setUp() throws Exception {
        store = FileStore.builder(folder.getRoot()).build();

        compactionMap1 = createCompactionMap(usePersistedMap);
        referenceMap1 = randomRecordIdMap(rnd, store.getTracker(), 10, 10);
        putAll(compactionMap1, referenceMap1);
        referenceMap.putAll(referenceMap1);

        compactionMap2 = createCompactionMap(usePersistedMap);
        referenceMap2 = randomRecordIdMap(rnd, store.getTracker(), 10, 10);
        putAll(compactionMap2, referenceMap2);
        referenceMap.putAll(referenceMap2);

        compactionMap3 = createCompactionMap(usePersistedMap);
        referenceMap3 = randomRecordIdMap(rnd, store.getTracker(), 10, 10);
        putAll(compactionMap3, referenceMap3);
        referenceMap.putAll(referenceMap3);

        this.compactionMap = CompactionMap.EMPTY.cons(compactionMap3).cons(compactionMap2).cons(compactionMap1);
    }

    @After
    public void tearDown() {
        store.close();
    }

    private PartialCompactionMap createCompactionMap(boolean persisted) {
        if (persisted) {
            return new PersistedCompactionMap(store.getTracker());
        } else {
            return new InMemoryCompactionMap(store.getTracker());
        }
    }

    public CompactionMapTest(boolean usePersistedMap) {
        this.usePersistedMap = usePersistedMap;
    }

    private static void putAll(PartialCompactionMap map1, Map<RecordId, RecordId> recordIdRecordIdMap) {
        for (Entry<RecordId, RecordId> tuple : recordIdRecordIdMap.entrySet()) {
            map1.put(tuple.getKey(), tuple.getValue());
        }
    }

    @Test
    public void checkExistingKeys() {
        for (Entry<RecordId, RecordId> reference : referenceMap.entrySet()) {
            assertEquals(reference.getValue(), compactionMap.get((reference.getKey())));
        }
    }

    @Test
    public void checkNonExistingKeys() {
        for (RecordId keys : randomRecordIdMap(rnd, store.getTracker(), 10, 10).keySet()) {
            if (!referenceMap.containsKey(keys)) {
                assertNull(compactionMap.get(keys));
            }
        }
    }

    @Test
    public void removeSome() {
        Set<UUID> removedUUIDs = newHashSet();
        for (int k = 0; k < 1 + rnd.nextInt(referenceMap.size()); k++) {
            RecordId key = get(referenceMap.keySet(), rnd.nextInt(referenceMap.size()));
            removedUUIDs.add(key.asUUID());
        }

        compactionMap.remove(removedUUIDs);

        for (Entry<RecordId, RecordId> reference : referenceMap.entrySet()) {
            RecordId key = reference.getKey();
            if (removedUUIDs.contains(key.asUUID())) {
                assertNull(compactionMap.get(key));
            } else {
                assertEquals(reference.getValue(), compactionMap.get(key));
            }
        }
    }

    private static long countUUIDs(Set<RecordId> recordIds) {
        Set<UUID> uuids = newHashSet();
        for (RecordId recordId : recordIds) {
            uuids.add(recordId.asUUID());
        }
        return uuids.size();
    }

    @Test
    public void removeGeneration() {
        compactionMap1.compress();
        compactionMap2.compress();
        compactionMap3.compress();

        assertArrayEquals(new long[]{10, 10, 10}, compactionMap.getSegmentCounts());
        assertArrayEquals(new long[] {100, 100, 100}, compactionMap.getRecordCounts());

        int expectedDepth = 3;
        int expectedGeneration = 3;
        long expectedSize = countUUIDs(referenceMap.keySet());
        assertEquals(expectedDepth, compactionMap.getDepth());
        assertEquals(expectedSize, sum(compactionMap.getSegmentCounts()));
        assertEquals(expectedGeneration, compactionMap.getGeneration());

        for (Map<RecordId, RecordId> referenceMap : ImmutableList.of(referenceMap2, referenceMap1, referenceMap3)) {
            Set<UUID> removedUUIDs = newHashSet();
            for (RecordId key : referenceMap.keySet()) {
                removedUUIDs.add(key.asUUID());
            }
            compactionMap.remove(removedUUIDs);
            expectedDepth--;
            // Effect of removed generation is only seen after subsequent cons. See OAK-3317
            CompactionMap consed = compactionMap.cons(compactionMap1);
            assertEquals(expectedDepth + 1, consed.getDepth());
            expectedSize -= removedUUIDs.size();
            assertEquals(expectedSize, sum(compactionMap.getSegmentCounts()));
            assertEquals(expectedGeneration + 1, consed.getGeneration());
        }

        // one final 'cons' to trigger cleanup of empty maps
        CompactionMap consed = compactionMap.cons(createCompactionMap(false));
        assertEquals(1, consed.getDepth());
        assertEquals(0, sum(compactionMap.getSegmentCounts()));
        assertEquals(expectedGeneration + 1, consed.getGeneration());
    }

    /**
     * See OAK-3511
     */
    @Test
    public void removeRecentKey() {
        compactionMap1.compress();

        // Find a key not present in the compaction map
        RecordId key = newRecordId(store.getTracker(), rnd);
        while (compactionMap1.get(key) != null) {
            key = newRecordId(store.getTracker(), rnd);
        }

        // Add it and immediately remove it, after which is should be gone
        compactionMap1.put(key, newRecordId(store.getTracker(), rnd));
        compactionMap1.remove(singleton(key.asUUID()));
        assertNull("Compaction map must not contain removed key", compactionMap1.get(key));
    }

}
