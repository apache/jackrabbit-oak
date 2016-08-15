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
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.benchmark.MicroBenchmark.run;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.plugins.segment.TestUtils.newValidOffset;
import static org.apache.jackrabbit.oak.plugins.segment.TestUtils.randomRecordIdMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.oak.commons.benchmark.MicroBenchmark.Benchmark;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is a unit test + benchmark test for all the compaction map
 * implementations.
 * </p>
 * <p>
 * The benchmark tests are <b>disabled</b> by default, to run one of them you
 * need to set the specific {@code benchmark.*} system property:<br>
 * {@code mvn test -Dtest.opts.memory=-Xmx5G -Dtest=PartialCompactionMapTest -Dbenchmark.benchLargeMap=true -Dbenchmark.benchPut=true -Dbenchmark.benchGet=true}
 * </p>
 */
@RunWith(Parameterized.class)
public class PartialCompactionMapTest {
    private static final Logger log = LoggerFactory.getLogger(PartialCompactionMapTest.class);
    private static final int SEED = Integer.getInteger("SEED", new Random().nextInt());

    private final Random rnd = new Random(SEED);
    private final boolean usePersistedMap;

    private FileStore segmentStore;

    private Map<RecordId, RecordId> reference;
    private PartialCompactionMap map;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Parameterized.Parameters
    public static List<Boolean[]> fixtures() {
        return ImmutableList.of(new Boolean[] {true}, new Boolean[] {false});
    }

    public PartialCompactionMapTest(boolean usePersistedMap) {
        this.usePersistedMap = usePersistedMap;
    }

    @Before
    public void setup() throws Exception {
        segmentStore = FileStore.builder(folder.getRoot()).build();
    }

    @After
    public void tearDown() {
        segmentStore.close();
    }

    private SegmentTracker getTracker() {
        return segmentStore.getTracker();
    }

    private PartialCompactionMap createCompactionMap() {
        SegmentWriter writer = new SegmentWriter(segmentStore, V_11, "");
        if (usePersistedMap) {
            return new PersistedCompactionMap(segmentStore.getTracker());
        } else {
            return new InMemoryCompactionMap(segmentStore.getTracker());
        }
    }

    private void addAll(Map<RecordId, RecordId> toAdd) {
        assert map != null;
        for (Entry<RecordId, RecordId> tuple : toAdd.entrySet()) {
            if (reference != null) {
                reference.put(tuple.getKey(), tuple.getValue());
            }
            map.put(tuple.getKey(), tuple.getValue());
        }
    }

    private void addRandomEntries(int segmentCount, int entriesPerSegment) {
        assert map != null;
        for (int k = 0; k < segmentCount / 1000; k++) {
            addAll(randomRecordIdMap(rnd, getTracker(), 1000, entriesPerSegment));
        }
        addAll(randomRecordIdMap(rnd, getTracker(), segmentCount % 1000, entriesPerSegment));
    }

    private void removeRandomEntries(int count) {
        assert reference != null;
        assert map != null;
        Set<SegmentId> remove = newHashSet();
        for (int k = 0; k < count && !reference.isEmpty(); k++) {
            int j = rnd.nextInt(reference.size());
            remove.add(get(reference.keySet(), j).getSegmentId());
        }

        Set<UUID> removeUUIDs = newHashSet();
        for (SegmentId sid : remove) {
            removeUUIDs.add(new UUID(sid.getMostSignificantBits(), sid.getLeastSignificantBits()));
            Iterator<RecordId> it = reference.keySet().iterator();
            while (it.hasNext()) {
                if (sid.equals(it.next().getSegmentId())) {
                    it.remove();
                }
            }
        }

        map.remove(removeUUIDs);
    }

    private void checkMap() {
        assert reference != null;
        assert map != null;
        for (Entry<RecordId, RecordId> entry : reference.entrySet()) {
            assertTrue("Failed with seed " + SEED,
                    map.wasCompactedTo(entry.getKey(), entry.getValue()));
            assertFalse("Failed with seed " + SEED,
                    map.wasCompactedTo(entry.getValue(), entry.getKey()));
        }
    }

    @Test
    public void single() {
        map = createCompactionMap();
        RecordId before = RecordId.fromString(getTracker(), "00000000-0000-0000-0000-000000000000.0000");
        RecordId after = RecordId.fromString(getTracker(),  "11111111-1111-1111-1111-111111111111.1111");

        map.put(before, after);
        assertEquals(after, map.get(before));
        map.compress();
        assertEquals(after, map.get(before));
        assertEquals(1, map.getRecordCount());
        assertEquals(1, map.getSegmentCount());
    }

    @Test
    public void remove() {
        map = createCompactionMap();
        RecordId before1 = RecordId.fromString(getTracker(), "00000000-0000-0000-0000-000000000000.0000");
        RecordId before2 = RecordId.fromString(getTracker(), "00000000-0000-0000-0000-000000000000.1111");
        RecordId after1 = RecordId.fromString(getTracker(),  "11111111-1111-1111-1111-111111111111.0000");
        RecordId after2 = RecordId.fromString(getTracker(),  "11111111-1111-1111-1111-111111111111.1111");

        map.put(before1, after1);
        map.compress();
        map.put(before2, after2);
        assertEquals(after1, map.get(before1));
        assertEquals(after2, map.get(before2));

        map.remove(newHashSet(before1.asUUID()));
        assertNull(map.get(before1));
        assertNull(map.get(before2));
        assertEquals(0, map.getRecordCount());
        assertEquals(0, map.getSegmentCount());
    }

    private static Set<UUID> toUUID(Set<RecordId> recordIds) {
        Set<UUID> uuids = newHashSet();
        for (RecordId recordId : recordIds) {
            uuids.add(recordId.asUUID());
        }
        return uuids;
    }

    @Test
    public void random() {
        int maxSegments = 1000;
        int entriesPerSegment = 10;
        reference = newHashMap();
        map = createCompactionMap();

        for (int k = 0; k < 10; k++) {
            addRandomEntries(rnd.nextInt(maxSegments) + 1, rnd.nextInt(entriesPerSegment) + 1);
            if (!reference.isEmpty()) {
                removeRandomEntries(rnd.nextInt(reference.size()));
            }
            checkMap();
        }
        map.compress();
        assertEquals(reference.size(), map.getRecordCount());
        assertEquals(toUUID(reference.keySet()).size(), map.getSegmentCount());
        checkMap();
    }

    private static void assertHeapSize(long size) {
        long mem = Runtime.getRuntime().maxMemory();
        assertTrue("Need " + humanReadableByteCount(size) +
            ", only found " + humanReadableByteCount(mem), mem >= size);
    }

    @Test
    public void benchLargeMap() {
        assumeTrue(Boolean.getBoolean("benchmark.benchLargeMap"));
        assertHeapSize(4000000000L);

        map = createCompactionMap();

        // check the memory use of really large mappings, 1M compacted segments with 10 records each.
        Runtime runtime = Runtime.getRuntime();
        for (int i = 0; i < 1000; i++) {
            Map<RecordId, RecordId> ids = randomRecordIdMap(rnd, getTracker(), 10000, 100);
            long start = System.nanoTime();
            for (Entry<RecordId, RecordId> entry : ids.entrySet()) {
                map.put(entry.getKey(), entry.getValue());
            }
            log.info(
                    "Bench Large Map #" +
                    (i + 1) + ": " + (runtime.totalMemory() - runtime.freeMemory()) /
                    (1024 * 1024) + "MB, " + (System.nanoTime() - start) / 1000000 + "ms");
        }
    }

    @Test
    public void benchPut() throws Exception {
        assumeTrue(Boolean.getBoolean("benchmark.benchPut"));
        assertHeapSize(4000000000L);

        run(new PutBenchmark(0, 100));
        run(new PutBenchmark(10, 100));
        run(new PutBenchmark(100, 100));
        run(new PutBenchmark(1000, 100));
        run(new PutBenchmark(10000, 100));
        run(new PutBenchmark(100000, 100));
        run(new PutBenchmark(1000000, 100));
    }

    @Test
    public void benchGet() throws Exception {
        assumeTrue(Boolean.getBoolean("benchmark.benchGet"));
        assertHeapSize(4000000000L);

        run(new GetBenchmark(0, 100));
        run(new GetBenchmark(10, 100));
        run(new GetBenchmark(100, 100));
        run(new GetBenchmark(1000, 100));
        run(new GetBenchmark(10000, 100));
        run(new GetBenchmark(100000, 100));
        run(new GetBenchmark(1000000, 100));
    }

    private abstract static class LoggingBenchmark extends Benchmark {

        @Override
        public void result(DescriptiveStatistics statistics) {
            log.info("{}", this);
            if (statistics.getN() > 0) {
                log.info(String
                        .format("%6s  %6s  %6s  %6s  %6s  %6s  %6s  %6s", "min",
                        "10%", "50%", "90%", "max", "mean", "stdev", "N"));
                log.info(String
                        .format("%6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d",
                                statistics.getMin() / 1000000,
                                statistics.getPercentile(10.0) / 1000000,
                                statistics.getPercentile(50.0) / 1000000,
                                statistics.getPercentile(90.0) / 1000000,
                                statistics.getMax() / 1000000,
                                statistics.getMean() / 1000000,
                                statistics.getStandardDeviation() / 1000000,
                                statistics.getN()));
            } else {
                log.info("No results");
            }
        }
    }

    private class PutBenchmark extends LoggingBenchmark {
        private final int segmentCount;
        private final int entriesPerSegment;

        private Map<RecordId, RecordId> putIds;

        public PutBenchmark(int segmentCount, int entriesPerSegment) {
            this.segmentCount = segmentCount;
            this.entriesPerSegment = entriesPerSegment;
        }

        @Override
        public void setup() throws IOException {
            map = createCompactionMap();
            if (segmentCount > 0) {
                addRandomEntries(segmentCount, entriesPerSegment);
            }
        }

        @Override
        public void beforeRun() throws Exception {
            putIds = randomRecordIdMap(rnd, getTracker(), 10000 / entriesPerSegment, entriesPerSegment);
        }

        @Override
        public void run() throws IOException {
            for (Entry<RecordId, RecordId> tuple : putIds.entrySet()) {
                map.put(tuple.getKey(), tuple.getValue());
            }
        }

        @Override
        public String toString() {
            return "Put benchmark: SegmentCount=" + segmentCount + ", entriesPerSegment=" + entriesPerSegment;
        }
    }

    private class GetBenchmark extends LoggingBenchmark {
        private final int segmentCount;
        private final int entriesPerSegment;

        private final List<RecordId> getCandidateIds = newArrayList();
        private final List<RecordId> getIds = newArrayList();

        public GetBenchmark(int segmentCount, int entriesPerSegment) {
            this.segmentCount = segmentCount;
            this.entriesPerSegment = entriesPerSegment;
        }

        @Override
        public void setup() throws IOException {
            map = createCompactionMap();
            reference = new HashMap<RecordId, RecordId>() {
                @Override
                public RecordId put(RecordId key, RecordId value) {
                    // Wow, what a horrendous hack!!
                    if (key.getSegmentId().getMostSignificantBits() % 10000 == 0) {
                        getCandidateIds.add(key);
                    }
                    return null;
                }
            };

            addRandomEntries(segmentCount, entriesPerSegment);
            map.compress();
            for (int k = 0; k < 10000; k++) {
                getCandidateIds.add(new RecordId(
                        getTracker().newDataSegmentId(),
                        newValidOffset(rnd, 0, MAX_SEGMENT_SIZE)));
            }
        }

        @Override
        public void beforeRun() throws Exception {
            for (int k = 0; k < 10000; k ++) {
                getIds.add(getCandidateIds.get(rnd.nextInt(getCandidateIds.size())));
            }
        }

        @Override
        public void run() {
            for (RecordId id : getIds) {
                map.get(id);
            }
        }

        @Override
        public void afterRun() throws Exception {
            getIds.clear();
        }

        @Override
        public String toString() {
            return "Get benchmark: segmentCount=" + segmentCount + ", entriesPerSegment=" + entriesPerSegment;
        }
    }

}
