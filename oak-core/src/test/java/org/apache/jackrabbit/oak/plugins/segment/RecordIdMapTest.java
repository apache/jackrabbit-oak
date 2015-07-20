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
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.encode;
import static org.apache.jackrabbit.oak.plugins.segment.TestUtils.newValidOffset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Test;

public class RecordIdMapTest {

    @Test
    public void testEmpty() {
        RecordIdMap map = new RecordIdMap();
        assertFalse(map.containsKey((short) 0));
        assertNull(map.get((short) 0));
        assertEquals(0, map.size());
        try {
            map.getKey(0);
            fail("Expected AIOBE");
        } catch (ArrayIndexOutOfBoundsException ignored) {}
        try {
            map.getRecordId(0);
            fail("Expected AIOBE");
        } catch (ArrayIndexOutOfBoundsException ignored) {}
    }

    @Test
    public void testRecordIdMap() {
        int maxSegments = 1000;
        int maxEntriesPerSegment = 10;
        int seed = new Random().nextInt();
        Random r = new Random(seed);

        SegmentTracker tracker = new MemoryStore().getTracker();
        RecordIdMap map = new RecordIdMap();
        Map<Short, RecordId> reference = newHashMap();
        int segments = r.nextInt(maxSegments);
        for (int i = 0; i < segments; i++) {
            SegmentId id = tracker.newDataSegmentId();
            int n = r.nextInt(maxEntriesPerSegment);
            int offset = MAX_SEGMENT_SIZE;
            for (int j = 0; j < n; j++) {
                offset = newValidOffset(r, (n - j) << RECORD_ALIGN_BITS, offset);
                RecordId record = new RecordId(id, offset);
                reference.put(encode(record.getOffset()), record);
            }
        }
        for (Entry<Short, RecordId> entry : reference.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }

        assertEquals("Failed with seed " + seed, reference.size(), map.size());
        for (Entry<Short, RecordId> entry : reference.entrySet()) {
            short key = entry.getKey();
            assertTrue("Failed with seed " + seed, map.containsKey(key));

            RecordId expected = entry.getValue();
            RecordId actual = map.get(key);
            assertEquals("Failed with seed " + seed, expected, actual);
        }

        for (int k = 0; k < map.size(); k++) {
            short key = map.getKey(k);
            RecordId expected = reference.get(key);
            RecordId actual = map.get(key);
            assertEquals("Failed with seed " + seed, expected, actual);
            assertEquals("Failed with seed " + seed, expected, map.getRecordId(k));
        }
    }
}
