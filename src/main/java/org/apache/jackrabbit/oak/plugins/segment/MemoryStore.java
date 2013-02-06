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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MemoryStore implements SegmentStore {

    private static final int MAX_SEGMENT_SIZE = 1 << 20; // 1MB

    private final Map<UUID, Segment> segments =
        Collections.synchronizedMap(new HashMap<UUID, Segment>());

    @Override
    public int getMaxSegmentSize() {
        return MAX_SEGMENT_SIZE;
    }

    @Override
    public Segment readSegment(UUID segmentId) {
        Segment segment = segments.get(segmentId);
        if (segment != null) {
            return segment;
        } else {
            throw new IllegalStateException("Segment not found: " + segmentId);
        }
    }

    @Override
    public void createSegment(Segment segment) {
        if (segments.put(segment.getSegmentId(), segment) != null) {
            throw new IllegalStateException(
                    "Segment override: " + segment.getSegmentId());
        }
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        byte[] segment = new byte[length];
        System.arraycopy(data, offset, segment, 0, length);
        createSegment(new Segment(segmentId, segment, new UUID[0]));
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (segments.remove(segmentId) == null) {
            throw new IllegalStateException("Missing segment: " + segmentId);
        }
    }

}
