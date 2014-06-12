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
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Maps.newHashMap;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.junit.Assert.assertFalse;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Test;

public class CompactionMapTest {

    public static void main(String[] args) {
        // check the memory use of really large mappings, 1M compacted
        // segments with 10 records each.
        Runtime runtime = Runtime.getRuntime();

        System.gc();
        System.out.println((runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024));

        CompactionMap map = new CompactionMap(100000);
        SegmentTracker factory = new MemoryStore().getTracker();
        for (int i = 0; i < 1000000; i++) {
            if (i % 1000 == 0) {
                System.gc();
                System.out.println(i + ": " + (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) + "MB");
            }
            SegmentId sid = factory.newDataSegmentId();
            for (int j = 0; j < 10; j++) {
                RecordId rid = new RecordId(sid, j << RECORD_ALIGN_BITS);
                map.put(rid, rid);
            }
        }

        System.gc();
        System.out.println("final: " + (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) + "MB");
    }

    @Test
    public void testCompactionMap() {
        int maxSegments = 1000;
        int maxEntriesPerSegment = 10;
        int seed = new Random().nextInt();
        Random r = new Random(seed);

        SegmentTracker factory = new MemoryStore().getTracker();
        CompactionMap map = new CompactionMap(r.nextInt(maxSegments / 2));
        Map<RecordId, RecordId> entries = newHashMap();

        int segments = r.nextInt(maxSegments);
        for (int i = 0; i < segments; i++) {
            SegmentId id = factory.newDataSegmentId();
            int n = r.nextInt(maxEntriesPerSegment);
            for (int j = 0; j < n; j++) {
                RecordId before = new RecordId(id, newValidOffset(r));
                RecordId after = new RecordId(factory.newDataSegmentId(), newValidOffset(r));
                entries.put(before, after);
                map.put(before, after);
                assertTrue("Failed with seed " + seed,
                        map.wasCompactedTo(before, after));
                assertFalse("Failed with seed " + seed,
                        map.wasCompactedTo(after, before));
            }
        }
        map.compress();

        for (Entry<RecordId, RecordId> entry : entries.entrySet()) {
            assertTrue("Failed with seed " + seed,
                    map.wasCompactedTo(entry.getKey(), entry.getValue()));
            assertFalse("Failed with seed " + seed,
                    map.wasCompactedTo(entry.getValue(), entry.getKey()));
        }
    }

    private int newValidOffset(Random random) {
        return random.nextInt(MAX_SEGMENT_SIZE >> RECORD_ALIGN_BITS)
                << RECORD_ALIGN_BITS;
    }
}
