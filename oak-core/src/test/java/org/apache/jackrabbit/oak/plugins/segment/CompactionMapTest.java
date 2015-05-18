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
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.inject.internal.util.$Sets.newHashSet;
import static java.io.File.createTempFile;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.commons.benchmark.MicroBenchmark.run;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.plugins.segment.file.FileStore.newFileStore;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.benchmark.MicroBenchmark.Benchmark;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CompactionMapTest {
    private static final boolean BENCH = Boolean.getBoolean("benchmark");
    private static final int SEED = Integer.getInteger("SEED", new Random().nextInt());

    private final Random rnd = new Random(SEED);

    private File directory;
    private SegmentStore segmentStore;

    private CompactionMap map;
    private Map<RecordId, RecordId> reference;

    @Before
    public void setup() throws IOException {
        directory = createTempFile(CompactionMapTest.class.getSimpleName(), "dir", new File("target"));
        directory.delete();
        directory.mkdir();

        segmentStore = newFileStore(directory).create();
        SegmentWriter writer = new SegmentWriter(segmentStore, getTracker(), V_11);
        map = new CompactionMap(100000, writer.getTracker());
        reference = newLinkedHashMap();
    }

    @After
    public void tearDown() {
        segmentStore.close();
        directory.delete();
    }

    private SegmentTracker getTracker() {
        return segmentStore.getTracker();
    }

    /**
     * Returns a new valid record offset, between {@code a} and {@code b},
     * exclusive.
     */
    private static int newValidOffset(Random random, int a, int b) {
        int p = (a >> RECORD_ALIGN_BITS) + 1;
        int q = (b >> RECORD_ALIGN_BITS);
        return (p + random.nextInt(q - p)) << RECORD_ALIGN_BITS;
    }

    private Map<RecordId, RecordId> randomMap(int maxSegments, int maxEntriesPerSegment) {
        Map<RecordId, RecordId> map = newHashMap();
        int segments = rnd.nextInt(maxSegments);
        for (int i = 0; i < segments; i++) {
            SegmentId id = getTracker().newDataSegmentId();
            int n = rnd.nextInt(maxEntriesPerSegment);
            int offset = MAX_SEGMENT_SIZE;
            for (int j = 0; j < n; j++) {
                offset = newValidOffset(rnd, (n - j) << RECORD_ALIGN_BITS, offset);
                RecordId before = new RecordId(id, offset);
                RecordId after = new RecordId(
                        getTracker().newDataSegmentId(),
                        newValidOffset(rnd, 0, MAX_SEGMENT_SIZE));
                map.put(before, after);
            }
        }
        return map;
    }

    private void addRandomEntries(int maxSegments, int maxEntriesPerSegment) {
        for (Entry<RecordId, RecordId> tuple : randomMap(maxSegments, maxEntriesPerSegment).entrySet()) {
            reference.put(tuple.getKey(), tuple.getValue());
            map.put(tuple.getKey(), tuple.getValue());
        }
    }

    private void removeRandomEntries(int count) {
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

        map.compress(removeUUIDs);
    }

    private void checkMap() {
        for (Entry<RecordId, RecordId> entry : reference.entrySet()) {
            assertTrue("Failed with seed " + SEED,
                    map.wasCompactedTo(entry.getKey(), entry.getValue()));
            assertFalse("Failed with seed " + SEED,
                    map.wasCompactedTo(entry.getValue(), entry.getKey()));
        }
    }

    @Test
    public void randomTest() {
        int maxSegments = 10000;
        int maxEntriesPerSegment = 10;

        for (int k = 0; k < 10; k++) {
            addRandomEntries(rnd.nextInt(maxSegments) + 1, rnd.nextInt(maxEntriesPerSegment) + 1);
            if (!reference.isEmpty()) {
                removeRandomEntries(rnd.nextInt(reference.size()));
            }
            checkMap();
        }
        map.compress();
        checkMap();
    }

    @Test
    public void benchLargeMap() {
        assumeTrue(BENCH);

        // check the memory use of really large mappings, 1M compacted segments with 10 records each.
        Runtime runtime = Runtime.getRuntime();
        Stopwatch timer = Stopwatch.createStarted();
        for (int i = 0; i < 1000000; i++) {
            if (i % 100000 == 0) {
                System.gc();
                System.out.println(
                        i + ": " + (runtime.totalMemory() - runtime.freeMemory()) /
                                (1024 * 1024) + "MB, " + timer.toString());
                timer.reset();
                timer.start();
            }
            SegmentId sid = getTracker().newDataSegmentId();
            for (int j = 0; j < 10; j++) {
                RecordId rid = new RecordId(sid, j << RECORD_ALIGN_BITS);
                map.put(rid, rid);
            }
        }
        map.compress();

        System.gc();
        System.out.println(
                "final: " + (runtime.totalMemory() - runtime.freeMemory()) /
                        (1024 * 1024) + "MB, " + timer.toString());
    }

    @Test
    public void benchPut() throws Exception {
        assumeTrue(BENCH);

        run(new PutBenchmark(0, 0));
        run(new PutBenchmark(1000, 10));
        run(new PutBenchmark(10000, 10));
        run(new PutBenchmark(100000, 10));
        run(new PutBenchmark(1000000, 10));
    }

    @Test
    public void benchGet() throws Exception {
        assumeTrue(BENCH);

        run(new GetBenchmark(1000, 10));
        run(new GetBenchmark(10000, 10));
        run(new GetBenchmark(100000, 10));
        run(new GetBenchmark(1000000, 10));
    }

    private class PutBenchmark extends Benchmark {
        private final int maxSegments;
        private final int maxEntriesPerSegment;

        private Map<RecordId, RecordId> putIds;

        public PutBenchmark(int maxSegments, int maxEntriesPerSegment) {
            this.maxSegments = maxSegments;
            this.maxEntriesPerSegment = maxEntriesPerSegment;
        }

        @Override
        public void setup() throws IOException {
            if (maxSegments > 0) {
                addRandomEntries(maxSegments, maxEntriesPerSegment);
            }
        }

        @Override
        public void beforeRun() throws Exception {
            putIds = randomMap(1000, 10);
        }

        @Override
        public void run() {
            for (Entry<RecordId, RecordId> tuple : putIds.entrySet()) {
                map.put(tuple.getKey(), tuple.getValue());
            }
        }

        @Override
        public String toString() {
            return "Put benchmark: maxSegments=" + maxSegments + ", maxEntriesPerSegment=" + maxEntriesPerSegment;
        }
    }

    private class GetBenchmark extends Benchmark {
        private final int maxSegments;
        private final int maxEntriesPerSegment;

        private final List<RecordId> getCandidateIds = newArrayList();
        private final List<RecordId> getIds = newArrayList();

        public GetBenchmark(int maxSegments, int maxEntriesPerSegment) {
            this.maxSegments = maxSegments;
            this.maxEntriesPerSegment = maxEntriesPerSegment;
        }

        @Override
        public void setup() throws IOException {
            addRandomEntries(maxSegments, maxEntriesPerSegment);
            map.compress();
            for (RecordId recordId : reference.keySet()) {
                if (rnd.nextInt(reference.size()) % 10000 == 0) {
                    getCandidateIds.add(recordId);
                }
            }
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
            return "Get benchmark: maxSegments=" + maxSegments + ", maxEntriesPerSegment=" + maxEntriesPerSegment;
        }
    }

}
