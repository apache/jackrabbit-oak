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
import com.google.common.io.ByteStreams;

public class SegmentWriter {

    private static final int INITIAL_BUFFER_SIZE = 1 << 12; // 4kB

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    static final int INLINE_BLOCKS = 16;

    static final int INLINE_SIZE = INLINE_BLOCKS * BLOCK_SIZE; // 64kB

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
    public RecordId writeString(String string) {
        byte[] data = string.getBytes(Charsets.UTF_8);
        List<RecordId> blockIds = new ArrayList<RecordId>();

        int headLength = Math.min(data.length, INLINE_SIZE);
        writeInlineBlocks(blockIds, data, 0, headLength);
        if (data.length > headLength) {
            int offset = headLength;
            while (offset + INLINE_SIZE <= data.length) {
                int bulkLength =
                    Math.min(data.length - offset, blockSegmentSize);
                writeBulkSegment(blockIds, data, offset, bulkLength);
                offset += bulkLength;
            }
            if (offset < data.length) {
                writeInlineBlocks(blockIds, data, offset, data.length - offset);
            }
        }

        return writeValueRecord(data.length, blockIds);
    }

    public RecordId writeStream(InputStream stream) throws IOException {
        List<RecordId> blockIds = new ArrayList<RecordId>();

        // First read the head of the stream. This covers most small
        // binaries and the frequently accessed head of larger ones.
        // The head gets inlined in the current segment.
        byte[] head = new byte[INLINE_SIZE];
        int headLength = ByteStreams.read(stream, head, 0, head.length);

        writeInlineBlocks(blockIds, head, 0, headLength);
        long length = headLength;

        // If the stream filled the full head buffer, it's likely that
        // the bulk of the data is still to come. Read it in larger
        // chunks and save in separate segments.
        if (headLength == head.length) {
            byte[] bulk = new byte[blockSegmentSize];
            int bulkLength = ByteStreams.read(stream, bulk, 0, bulk.length);
            while (bulkLength > INLINE_SIZE) {
                writeBulkSegment(blockIds, bulk, 0, bulkLength);
                length += bulkLength;
                bulkLength = ByteStreams.read(stream, bulk, 0, bulk.length);
            }
            // The tail chunk of the stream is too small to put in a separate
            // segment, so we inline also it.
            if (bulkLength > 0) {
                writeInlineBlocks(blockIds, bulk, 0, bulkLength);
                length += bulkLength;
            }
        }

        return writeValueRecord(length, blockIds);
    }

    private RecordId writeValueRecord(long length, List<RecordId> blockIds) {
        // Store the list of blocks along with the length of the value
        RecordId listId = writeList(blockIds);
        RecordId valueId = prepare(8, Collections.singleton(listId));
        buffer.putLong(length);
        writeRecordId(listId);
        return valueId;
    }

    private void writeInlineBlocks(
            List<RecordId> blockIds, byte[] buffer, int offset, int length) {
        int begin = offset;
        int end = offset + length;
        while (begin + BLOCK_SIZE <= end) {
            blockIds.add(writeBlock(buffer, begin, BLOCK_SIZE));
            begin += BLOCK_SIZE;
        }
        if (begin < end) {
            blockIds.add(writeBlock(buffer, begin, end - begin));
        }
    }

    private void writeBulkSegment(
            List<RecordId> blockIds, byte[] buffer, int offset, int length) {
        UUID segmentId = UUID.randomUUID();
        store.createSegment(segmentId, buffer, offset, length);
        for (int position = 0; position < length; position += BLOCK_SIZE) {
            blockIds.add(new RecordId(segmentId, position));
        }
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
