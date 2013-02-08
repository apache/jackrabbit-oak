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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

    public synchronized void flush() {
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

    private synchronized void writeRecordId(RecordId id) {
        UUID segmentId = id.getSegmentId();
        int index = uuids.indexOf(segmentId);
        if (index == -1) {
            index = uuids.size();
            uuids.add(segmentId);
        }
        buffer.putInt(index << 24 | id.getOffset());
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

    private synchronized RecordId writeListBucket(List<RecordId> bucket) {
        RecordId bucketId = prepare(0, bucket);
        for (RecordId id : bucket) {
            writeRecordId(id);
        }
        return bucketId;
    }

    class MapEntry implements Comparable<MapEntry> {
        private final int hashCode;
        private final RecordId key;
        private final RecordId value;

        MapEntry(int hashCode, RecordId key, RecordId value) {
            this.hashCode = hashCode;
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(MapEntry that) {
            int diff = hashCode - that.hashCode;
            if (diff == 0) {
                diff = key.compareTo(that.key);
            }
            return diff;
        }
    }

    private synchronized RecordId writeMapBucket(
            List<MapEntry> entries, int level) {
        int size = 1 << MapRecord.LEVEL_BITS;
        int mask = size - 1;
        int shift = level * MapRecord.LEVEL_BITS;

        if (entries.size() <= size) {
            Collections.sort(entries);

            List<RecordId> ids = Lists.newArrayList();
            for (MapEntry entry : entries) {
                ids.add(entry.key);
                ids.add(entry.value);
            }

            RecordId bucketId = prepare(4 + entries.size() * 12, ids);
            buffer.putInt(entries.size());
            for (MapEntry entry : entries) {
                buffer.putInt(entry.hashCode);
            }
            for (MapEntry entry : entries) {
                writeRecordId(entry.key);
            }
            for (MapEntry entry : entries) {
                writeRecordId(entry.value);
            }
            return bucketId;
        } else {
            List<MapEntry>[] buckets = new List[size];
            for (MapEntry entry : entries) {
                int bucketIndex = (entry.hashCode >> shift) & mask;
                if (buckets[bucketIndex] == null) {
                    buckets[bucketIndex] = Lists.newArrayList();
                }
                buckets[bucketIndex].add(entry);
            }

            List<RecordId> bucketIds = Lists.newArrayList();
            long bucketMap = 0L;
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] != null) {
                    bucketIds.add(writeMapBucket(buckets[i], level + 1));
                    bucketMap |= 1L << i;
                }
            }

            RecordId bucketId = prepare(12 + bucketIds.size() * 4, bucketIds);
            buffer.putInt(entries.size());
            buffer.putLong(bucketMap);
            for (RecordId id : bucketIds) {
                writeRecordId(id);
            }
            return bucketId;
        }
    }

    private synchronized RecordId writeValueRecord(
            long length, RecordId blocks) {
        RecordId valueId = prepare(8, Collections.singleton(blocks));
        buffer.putLong(length);
        writeRecordId(blocks);
        return valueId;
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
     * @param list list of record identifiers
     * @return list record identifier
     */
    public RecordId writeList(List<RecordId> list) {
        checkNotNull(list);

        if (list.isEmpty()) {
            return prepare(0); // special case
        }

        List<RecordId> thisLevel = list;
        while (thisLevel.size() > 1) {
            List<RecordId> nextLevel = Lists.newArrayList();
            for (List<RecordId> bucket :
                    Lists.partition(thisLevel, ListRecord.LEVEL_SIZE)) {
                nextLevel.add(writeListBucket(bucket));
            }
            thisLevel = nextLevel;
        }
        return thisLevel.iterator().next();
    }

    public RecordId writeMap(Map<String, RecordId> map) {
        List<MapEntry> entries = Lists.newArrayList();
        for (Map.Entry<String, RecordId> entry : map.entrySet()) {
            String key = entry.getKey();
            entries.add(new MapEntry(
                    key.hashCode(), writeString(key), entry.getValue()));
        }
        return writeMapBucket(entries, 0);
    }

    /**
     * Writes a string value record.
     *
     * @param string string to be written
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

        return writeValueRecord(data.length, writeList(blockIds));
    }

    /**
     * Writes a stream value record. The given stream is consumed
     * <em>and closed</em> by this method.
     *
     * @param stream stream to be written
     * @return value record identifier
     * @throws IOException if the stream could not be read
     */
    public RecordId writeStream(InputStream stream) throws IOException {
        try {
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
                // The tail chunk of the stream is too small to put in
                // a separate segment, so we inline also it.
                if (bulkLength > 0) {
                    writeInlineBlocks(blockIds, bulk, 0, bulkLength);
                    length += bulkLength;
                }
            }

            return writeValueRecord(length, writeList(blockIds));
        } finally {
            stream.close();
        }
    }

    private RecordId writeProperty(PropertyState state) {
        Type<?> type = state.getType();
        int count = state.count();

        List<RecordId> valueIds = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            if (type.tag() == PropertyType.BINARY) {
                try {
                    valueIds.add(writeStream(
                            state.getValue(Type.BINARY, i).getNewStream()));
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected IOException", e);
                }
            } else {
                valueIds.add(writeString(state.getValue(Type.STRING, i)));
            }
        }
        RecordId valueId = writeList(valueIds);

        RecordId propertyId = prepare(8, Collections.singleton(valueId));
        buffer.putInt(type.tag());
        if (state.isArray()) {
            buffer.putInt(count);
        } else {
            buffer.putInt(-1);
        }
        writeRecordId(valueId);
        return propertyId;
    }

    public RecordId writeNode(NodeState state) {
        Map<String, RecordId> childNodes = Maps.newHashMap();
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            childNodes.put(entry.getName(), writeNode(entry.getNodeState()));
        }
        RecordId childNodesId = writeMap(childNodes);

        Map<String, RecordId> properties = Maps.newHashMap();
        for (PropertyState property : state.getProperties()) {
            properties.put(property.getName(), writeProperty(property));
        }
        RecordId propertiesId = writeMap(properties);

        RecordId id = prepare(0, ImmutableList.of(propertiesId, childNodesId));
        writeRecordId(propertiesId);
        writeRecordId(childNodesId);
        return id;
    }

}
