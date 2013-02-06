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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class SegmentWriter {

    private static final int INITIAL_BUFFER_SIZE = 1 << 12; // 4kB

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    static final int INLINE_LIMIT = 16 * BLOCK_SIZE; // 64kB

    private final SegmentStore store;

    private final int blocksPerSegment;

    private final int blockSegmentSize;

    private UUID uuid = UUID.randomUUID();

    private List<UUID> uuids = new ArrayList<UUID>(255);

    private ByteBuffer buffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);

    public SegmentWriter(SegmentStore store) {
        this.store = store;
        this.blocksPerSegment = store.getMaxSegmentSize() / BLOCK_SIZE;
        this.blockSegmentSize = blocksPerSegment * BLOCK_SIZE;
    }

    public void flush() {
        if (buffer.position() > 0) {
            byte[] data = new byte[buffer.position()];
            buffer.flip();
            buffer.get(data);

            store.createSegment(new Segment(
                    uuid, data, uuids.toArray(new UUID[0])));

            uuid = UUID.randomUUID();
            uuids.clear();
            buffer.clear();
        }
    }

    private RecordId prepare(int size) {
        return prepare(size, Collections.<RecordId>emptyList());
    }

    private synchronized RecordId prepare(int size, Collection<RecordId> ids) {
        Set<UUID> segmentIds = new HashSet<UUID>();
        for (RecordId id : ids) {
            UUID segmentId = id.getSegmentId();
            if (!uuid.equals(segmentId) && !uuids.contains(segmentId)) {
                segmentIds.add(segmentId);
            }
        }

        int fullSize = size + 4 * ids.size();
        if (buffer.position() + fullSize > store.getMaxSegmentSize()) {
            flush();
        }
        if (fullSize > buffer.remaining()) {
            int n = Math.min(buffer.capacity() * 2, store.getMaxSegmentSize());
            ByteBuffer newBuffer = ByteBuffer.allocate(n);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
        return new RecordId(uuid, buffer.position());
    }

    /**
     * Writes a block record containing the given block of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes to write
     * @return block record identifier
     */
    public synchronized RecordId writeBlock(
            byte[] bytes, int offset, int length) {
        checkNotNull(bytes);
        checkPositionIndexes(offset, offset + length, bytes.length);

        RecordId blockId = prepare(length);
        buffer.put(bytes, offset, length);
        return blockId;
    }

    /**
     * Writes a list record containing the given list of record identifiers.
     *
     * @param ids list of record identifiers
     * @return list record identifier
     */
    public synchronized RecordId writeList(List<RecordId> ids) {
        checkNotNull(ids);

        int size = ids.size();
        if (size == 0) {
            return prepare(0);
        } else if (size == 1) {
            return ids.iterator().next();
        } else {
            List<RecordId> thisLevel = ids;
            do {
                List<RecordId> nextLevel = new ArrayList<RecordId>();
                for (List<RecordId> bucket :
                        Lists.partition(thisLevel, ListRecord.LEVEL_SIZE)) {
                    RecordId bucketId = prepare(0, bucket);
                    for (RecordId id : bucket) {
                        writeRecordId(id);
                    }
                    nextLevel.add(bucketId);
                }
                thisLevel = nextLevel;
            } while (thisLevel.size() > 1);
            return thisLevel.iterator().next();
        }
    }

    /**
     * Writes a value record containing the given sequence of bytes.
     *
     * @param bytes source buffer
     * @param offset offset within the source buffer
     * @param length number of bytes in the value
     * @return value record identifier
     */
    public synchronized RecordId writeValue(
            byte[] bytes, int offset, int length) {
        List<RecordId> blockIds = new ArrayList<RecordId>();
        int remaining = length;

        // First create block segments for a large bulk of the value
        while (remaining > INLINE_LIMIT) {
            int len = Math.min(remaining, blockSegmentSize);
            UUID segmentId = UUID.randomUUID();
            store.createSegment(segmentId, bytes, offset, len);
            for (int position = 0; position < len; position += BLOCK_SIZE) {
                blockIds.add(new RecordId(segmentId, position));
            }
            offset += len;
            remaining -= len;
        }

        // Then inline any remaining full blocks
        while (remaining > BLOCK_SIZE) {
            blockIds.add(writeBlock(bytes, offset, BLOCK_SIZE));
            offset += BLOCK_SIZE;
            remaining -= BLOCK_SIZE;
        }

        // Finally add the last partial block (if any)
        if (remaining > 0) {
            blockIds.add(writeBlock(bytes, offset, remaining));
        }

        // Store the list of blocks along with the length of the value
        RecordId listId = writeList(blockIds);
        RecordId valueId = prepare(8, Collections.singleton(listId));
        buffer.putLong(length);
        writeRecordId(listId);
        return valueId;
    }

    public RecordId writeString(String string) {
        byte[] data = string.getBytes(Charsets.UTF_8);
        return writeValue(data, 0, data.length);
    }

    public RecordId writeStream(InputStream stream) throws IOException {
        throw new UnsupportedOperationException(); // TODO
    }

    private void writeRecordId(RecordId id) {
        UUID segmentId = id.getSegmentId();
        int index = uuids.indexOf(segmentId);
        if (index == -1) {
            index = uuids.size();
            uuids.add(segmentId);
        }
        buffer.putInt(index << 24 | id.getOffset());
    }

}
