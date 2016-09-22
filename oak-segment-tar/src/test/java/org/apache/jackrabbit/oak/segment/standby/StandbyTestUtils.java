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

package org.apache.jackrabbit.oak.segment.standby;

import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.hash.Hashing;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;

public class StandbyTestUtils {

    private StandbyTestUtils() {
        // Prevent instantiation.
    }

    public static RecordId mockRecordId(long msb, long lsb, int offset) {
        return new RecordId(new SegmentId(mock(SegmentStore.class), msb, lsb), offset);
    }

    public static Segment mockSegment(UUID uuid, byte[] buffer) {
        SegmentStore store = mock(SegmentStore.class);
        SegmentReader reader = mock(SegmentReader.class);
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        SegmentId id = new SegmentId(store, msb, lsb);
        ByteBuffer data = ByteBuffer.wrap(buffer);
        return new Segment(store, reader, id, data);
    }

    public static long hash(byte[] data) {
        return Hashing.murmur3_32().newHasher().putBytes(data).hash().padToLong();
    }

}
