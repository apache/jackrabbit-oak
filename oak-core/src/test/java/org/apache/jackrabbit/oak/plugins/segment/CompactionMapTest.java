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

import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Test;

public class CompactionMapTest {

    @Test
    public void testCompactionMap() {
        int maxSegments = 1000;
        int maxEntriesPerSegment = 10;
        int seed = new Random().nextInt();

        SegmentTracker factory = new MemoryStore().getTracker();
        Map<RecordId, RecordId> map = new HashMap<RecordId, RecordId>();

        Random r = new Random(seed);
        int segments = r.nextInt(maxSegments);
        for (int i = 0; i < segments; i++) {
            SegmentId id = factory.newDataSegmentId();
            int entries = r.nextInt(maxEntriesPerSegment);
            for (int j = 0; j < entries; j++) {
                map.put(new RecordId(id, newValidOffset(r)),
                        new RecordId(factory.newDataSegmentId(), newValidOffset(r)));
            }
        }

        CompactionMap compaction = new CompactionMap(map);
        for (Entry<RecordId, RecordId> e : map.entrySet()) {
            assertTrue("Failed with seed " + seed,
                    compaction.wasCompactedTo(e.getKey(), e.getValue()));
            assertFalse("Failed with seed " + seed,
                    compaction.wasCompactedTo(e.getValue(), e.getKey()));
        }
    }

    private int newValidOffset(Random random) {
        return random.nextInt(MAX_SEGMENT_SIZE >> RECORD_ALIGN_BITS)
                << RECORD_ALIGN_BITS;
    }
}
