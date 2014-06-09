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

import static org.apache.jackrabbit.oak.plugins.segment.Compactor.mapToByteBuffer;
import static org.apache.jackrabbit.oak.plugins.segment.Compactor.readEntry;
import static org.apache.jackrabbit.oak.plugins.segment.Compactor.recordAsKey;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Test;

public class CompactorTest {

    @Test
    public void mapSerializationTest() {

        final int maxExistingEntries = 100000;
        final int maxNonExistingEntries = 10000;
        final int seed = new Random().nextInt();

        SegmentTracker factory = new MemoryStore().getTracker();
        Map<RecordId, RecordId> map = new HashMap<RecordId, RecordId>();

        Random r = new Random(seed);
        int existing = r.nextInt(maxExistingEntries);
        int nonExisting = r.nextInt(maxNonExistingEntries);

        for (int i = 0; i < existing; i++) {
            RecordId k = new RecordId(factory.newDataSegmentId(),
                    asValidOffset(r.nextInt(MAX_SEGMENT_SIZE)));
            RecordId v = new RecordId(factory.newDataSegmentId(),
                    asValidOffset(r.nextInt(MAX_SEGMENT_SIZE)));
            map.put(k, v);
        }
        ByteBuffer compaction = mapToByteBuffer(map);

        // not serialized, expecting the same value back
        for (int i = 0; i < nonExisting; i++) {
            RecordId k = new RecordId(factory.newDataSegmentId(),
                    asValidOffset(r.nextInt(MAX_SEGMENT_SIZE)));
            assertFalse("Clash on recordids", map.containsKey(k));
            map.put(k, k);
        }

        for (Entry<RecordId, RecordId> e : map.entrySet()) {
            long[] v = recordAsKey(e.getValue());
            long[] vl = readEntry(compaction, e.getKey());
            assertArrayEquals("Failed with seed " + seed, vl, v);
        }
    }

    private int asValidOffset(int random) {
        while (random > 0) {
            if (random % (1 << RECORD_ALIGN_BITS) == 0) {
                return random;
            }
            random--;
        }
        return random;
    }
}
