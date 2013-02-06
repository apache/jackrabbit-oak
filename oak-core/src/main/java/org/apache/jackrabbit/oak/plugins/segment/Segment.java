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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.cache.Weigher;

class Segment {

    static final int SMALL_LIMIT = 1 << 7;

    static final int MEDIUM_LIMIT = 1 << 14 + SMALL_LIMIT;

    private final UUID uuid;

    private final byte[] data;

    private final UUID[] uuids;

    Segment(UUID uuid, byte[] data, UUID[] uuids) {
        this.uuid = uuid;
        this.data = data;
        this.uuids = uuids;
    }

    public UUID getSegmentId() {
        return uuid;
    }

    public int size() {
        return data.length;
    }

    /**
     * Reads the given number of bytes starting from the given position
     * in this segment.
     *
     * @param position position within segment
     * @param buffer target buffer
     * @param offset offset within target buffer
     * @param length number of bytes to read
     */
    public void readBytes(int position, byte[] buffer, int offset, int length) {
        checkPositionIndexes(position, position + length, data.length);
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);

        System.arraycopy(data, position, buffer, offset, length);
    }

    RecordId readRecordId(int offset) {
        return new RecordId(
                uuids[data[offset] & 0xff],
                (data[offset + 1] & 0xff) << 16
                | (data[offset + 2] & 0xff) << 8
                | (data[offset + 3] & 0xff));
    }

    public long readLength(int offset) {
        checkPositionIndexes(offset, offset + 8, data.length);
        return ByteBuffer.wrap(data).getLong(offset);
    }

    public static Weigher<UUID, Segment> weigher() {
        return new Weigher<UUID, Segment>() {
            @Override
            public int weigh(UUID key, Segment value) {
                return value.size();
            }
        };
    }

}
