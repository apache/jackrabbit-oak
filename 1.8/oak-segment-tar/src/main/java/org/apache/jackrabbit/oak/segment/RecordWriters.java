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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Arrays.sort;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.segment.MapRecord.SIZE_BITS;
import static org.apache.jackrabbit.oak.segment.RecordType.BLOB_ID;
import static org.apache.jackrabbit.oak.segment.RecordType.BLOCK;
import static org.apache.jackrabbit.oak.segment.RecordType.BRANCH;
import static org.apache.jackrabbit.oak.segment.RecordType.BUCKET;
import static org.apache.jackrabbit.oak.segment.RecordType.LEAF;
import static org.apache.jackrabbit.oak.segment.RecordType.LIST;
import static org.apache.jackrabbit.oak.segment.RecordType.NODE;
import static org.apache.jackrabbit.oak.segment.RecordType.TEMPLATE;
import static org.apache.jackrabbit.oak.segment.RecordType.VALUE;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_ID_BYTES;
import static org.apache.jackrabbit.oak.segment.Segment.SMALL_LIMIT;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

final class RecordWriters {
    private RecordWriters() {}

    /**
     * Base class for all record writers
     */
    public abstract static class RecordWriter {
        private final RecordType type;
        protected final int size;
        protected final Collection<RecordId> ids;

        protected RecordWriter(RecordType type, int size,
                Collection<RecordId> ids) {
            this.type = type;
            this.size = size;
            this.ids = ids;
        }

        protected RecordWriter(RecordType type, int size, RecordId id) {
            this(type, size, singleton(id));
        }

        protected RecordWriter(RecordType type, int size) {
            this(type, size, Collections.<RecordId> emptyList());
        }

        public final RecordId write(SegmentBufferWriter writer, SegmentStore store) throws IOException {
            RecordId id = writer.prepare(type, size, ids, store);
            return writeRecordContent(id, writer);
        }

        protected abstract RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer);
    }

    public static RecordWriter newMapLeafWriter(int level, Collection<MapEntry> entries) {
        return new MapLeafWriter(level, entries);
    }

    public static RecordWriter newMapLeafWriter() {
        return new MapLeafWriter();
    }

    public static RecordWriter newMapBranchWriter(int level, int entryCount, int bitmap, List<RecordId> ids) {
        return new MapBranchWriter(level, entryCount, bitmap, ids);
    }

    public static RecordWriter newMapBranchWriter(int bitmap, List<RecordId> ids) {
        return new MapBranchWriter(bitmap, ids);
    }

    public static RecordWriter newListWriter(int count, RecordId lid) {
        return new ListWriter(count, lid);
    }

    public static RecordWriter newListWriter() {
        return new ListWriter();
    }

    public static RecordWriter newListBucketWriter(List<RecordId> ids) {
        return new ListBucketWriter(ids);
    }

    public static RecordWriter newBlockWriter(byte[] bytes, int offset, int length) {
        return new BlockWriter(bytes, offset, length);
    }

    public static RecordWriter newValueWriter(RecordId rid, long len) {
        return new SingleValueWriter(rid, len);
    }

    public static RecordWriter newValueWriter(int length, byte[] data) {
        return new ArrayValueWriter(length, data);
    }

    /**
     * Write a large blob ID. A blob ID is considered large if the length of its
     * binary representation is equal to or greater than {@code
     * Segment.BLOB_ID_SMALL_LIMIT}.
     */
    public static RecordWriter newBlobIdWriter(RecordId rid) {
        return new LargeBlobIdWriter(rid);
    }

    /**
     * Write a small blob ID. A blob ID is considered small if the length of its
     * binary representation is less than {@code Segment.BLOB_ID_SMALL_LIMIT}.
     */
    public static RecordWriter newBlobIdWriter(byte[] blobId) {
        return new SmallBlobIdWriter(blobId);
    }

    public static RecordWriter newTemplateWriter(Collection<RecordId> ids,
            RecordId[] propertyNames, byte[] propertyTypes, int head, RecordId primaryId,
            List<RecordId> mixinIds, RecordId childNameId, RecordId propNamesId) {
        return new TemplateWriter(ids, propertyNames, propertyTypes, head, primaryId, mixinIds,
            childNameId, propNamesId);
    }

    public static RecordWriter newNodeStateWriter(RecordId stableId, List<RecordId> ids) {
        return new NodeStateWriter(stableId, ids);
    }

    /**
     * Map Leaf record writer.
     * @see RecordType#LEAF
     */
    private static class MapLeafWriter extends RecordWriter {
        private final int level;
        private final Collection<MapEntry> entries;

        private MapLeafWriter() {
            super(LEAF, 4);
            this.level = -1;
            this.entries = null;
        }

        private MapLeafWriter(int level, Collection<MapEntry> entries) {
            super(LEAF, 4 + entries.size() * 4, extractIds(entries));
            this.level = level;
            this.entries = entries;
        }

        private static List<RecordId> extractIds(Collection<MapEntry> entries) {
            List<RecordId> ids = newArrayListWithCapacity(2 * entries.size());
            for (MapEntry entry : entries) {
                ids.add(entry.getKey());
                ids.add(entry.getValue());
            }
            return ids;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            if (entries != null) {
                int size = entries.size();
                writer.writeInt((level << SIZE_BITS) | size);

                // copy the entries to an array so we can sort them before
                // writing
                MapEntry[] array = entries.toArray(new MapEntry[size]);
                sort(array);

                for (MapEntry entry : array) {
                    writer.writeInt(entry.getHash());
                }
                for (MapEntry entry : array) {
                    writer.writeRecordId(entry.getKey());
                    writer.writeRecordId(entry.getValue());
                }
            } else {
                writer.writeInt(0);
            }
            return id;
        }
    }

    /**
     * Map Branch record writer.
     * @see RecordType#BRANCH
     */
    private static class MapBranchWriter extends RecordWriter {
        private final int level;
        private final int entryCount;
        private final int bitmap;

        /*
         * Write a regular map branch
         */
        private MapBranchWriter(int level, int entryCount, int bitmap, List<RecordId> ids) {
            super(BRANCH, 8, ids);
            this.level = level;
            this.entryCount = entryCount;
            this.bitmap = bitmap;
        }

        /*
         * Write a diff map
         */
        private MapBranchWriter(int bitmap, List<RecordId> ids) {
            // level = 0 and and entryCount = -1 -> this is a map diff
            this(0, -1, bitmap, ids);
        }

        @Override
        protected RecordId writeRecordContent(RecordId id, SegmentBufferWriter writer) {
            // -1 to encode a map diff (if level == 0 and entryCount == -1)
            writer.writeInt((level << SIZE_BITS) | entryCount);
            writer.writeInt(bitmap);
            for (RecordId mapId : ids) {
                writer.writeRecordId(mapId);
            }
            return id;
        }
    }

    /**
     * List record writer.
     * @see RecordType#LIST
     */
    private static class ListWriter extends RecordWriter  {
        private final int count;
        private final RecordId lid;

        private ListWriter() {
            super(LIST, 4);
            count = 0;
            lid = null;
        }

        private ListWriter(int count, RecordId lid) {
            super(LIST, 4, lid);
            this.count = count;
            this.lid = lid;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            writer.writeInt(count);
            if (lid != null) {
                writer.writeRecordId(lid);
            }
            return id;
        }
    }

    /**
     * List Bucket record writer.
     *
     * @see RecordType#BUCKET
     */
    private static class ListBucketWriter extends RecordWriter {

        private ListBucketWriter(List<RecordId> ids) {
            super(BUCKET, 0, ids);
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            for (RecordId bucketId : ids) {
                writer.writeRecordId(bucketId);
            }
            return id;
        }
    }

    /**
     * Block record writer.
     * @see RecordType#BLOCK
     */
    private static class BlockWriter extends RecordWriter {
        private final byte[] bytes;
        private final int offset;

        private BlockWriter(byte[] bytes, int offset, int length) {
            super(BLOCK, length);
            this.bytes = bytes;
            this.offset = offset;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            writer.writeBytes(bytes, offset, size);
            return id;
        }
    }

    /**
     * Single RecordId record writer.
     * @see RecordType#VALUE
     */
    private static class SingleValueWriter extends RecordWriter {
        private final RecordId rid;
        private final long len;

        private SingleValueWriter(RecordId rid, long len) {
            super(VALUE, 8, rid);
            this.rid = rid;
            this.len = len;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            writer.writeLong(len);
            writer.writeRecordId(rid);
            return id;
        }
    }

    /**
     * Bye array record writer. Used as a special case for short binaries (up to
     * about {@code Segment#MEDIUM_LIMIT}): store them directly as small or
     * medium-sized value records.
     * @see Segment#MEDIUM_LIMIT
     * @see RecordType#VALUE
     */
    private static class ArrayValueWriter extends RecordWriter {
        private final int length;
        private final byte[] data;

        private ArrayValueWriter(int length, byte[] data) {
            super(VALUE, length + getSizeDelta(length));
            this.length = length;
            this.data = data;
        }

        private static boolean isSmallSize(int length) {
            return length < SMALL_LIMIT;
        }

        private static int getSizeDelta(int length) {
            if (isSmallSize(length)) {
                return 1;
            } else {
                return 2;
            }
        }

        @Override
        protected RecordId writeRecordContent(RecordId id, SegmentBufferWriter writer) {
            if (isSmallSize(length)) {
                writer.writeByte((byte) length);
            } else {
                writer.writeShort((short) ((length - SMALL_LIMIT) | 0x8000));
            }
            writer.writeBytes(data, 0, length);
            return id;
        }
    }

    /**
     * Large Blob record writer. A blob ID is considered large if the length of
     * its binary representation is equal to or greater than
     * {@code Segment#BLOB_ID_SMALL_LIMIT}.
     *
     * @see Segment#BLOB_ID_SMALL_LIMIT
     * @see RecordType#BLOB_ID
     */
    private static class LargeBlobIdWriter extends RecordWriter {
        private final RecordId stringRecord;

        private LargeBlobIdWriter(RecordId stringRecord) {
            super(BLOB_ID, 1, stringRecord);
            this.stringRecord = stringRecord;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            // The length uses a fake "length" field that is always equal to
            // 0xF0.
            // This allows the code to take apart small from a large blob IDs.
            writer.writeByte((byte) 0xF0);
            writer.writeRecordId(stringRecord);
            return id;
        }
    }

    /**
     * Small Blob record writer. A blob ID is considered small if the length of
     * its binary representation is less than {@code Segment#BLOB_ID_SMALL_LIMIT}.

     * @see Segment#BLOB_ID_SMALL_LIMIT
     * @see RecordType#BLOB_ID
     */
    private static class SmallBlobIdWriter extends RecordWriter {
        private final byte[] blobId;

        private SmallBlobIdWriter(byte[] blobId) {
            super(BLOB_ID, 2 + blobId.length);
            checkArgument(blobId.length < Segment.BLOB_ID_SMALL_LIMIT);
            this.blobId = blobId;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            int length = blobId.length;
            writer.writeShort((short) (length | 0xE000));
            writer.writeBytes(blobId, 0, length);
            return id;
        }
    }

    /**
     * Template record writer.
     * @see RecordType#TEMPLATE
     */
    private static class TemplateWriter extends RecordWriter {
        private final RecordId[] propertyNames;
        private final byte[] propertyTypes;
        private final int head;
        private final RecordId primaryId;
        private final List<RecordId> mixinIds;
        private final RecordId childNameId;
        private final RecordId propNamesId;

        private TemplateWriter(Collection<RecordId> ids, RecordId[] propertyNames,
                byte[] propertyTypes, int head, RecordId primaryId, List<RecordId> mixinIds,
                RecordId childNameId, RecordId propNamesId) {
            super(TEMPLATE, 4 + propertyTypes.length, ids);
            this.propertyNames = propertyNames;
            this.propertyTypes = propertyTypes;
            this.head = head;
            this.primaryId = primaryId;
            this.mixinIds = mixinIds;
            this.childNameId = childNameId;
            this.propNamesId = propNamesId;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id,
                SegmentBufferWriter writer) {
            writer.writeInt(head);
            if (primaryId != null) {
                writer.writeRecordId(primaryId);
            }
            if (mixinIds != null) {
                for (RecordId mixinId : mixinIds) {
                    writer.writeRecordId(mixinId);
                }
            }
            if (childNameId != null) {
                writer.writeRecordId(childNameId);
            }
            if (propNamesId != null) {
                writer.writeRecordId(propNamesId);
            }
            for (int i = 0; i < propertyNames.length; i++) {
                writer.writeByte(propertyTypes[i]);
            }
            return id;
        }
    }

    /**
     * Node State record writer.
     * @see RecordType#NODE
     */
    private static class NodeStateWriter extends RecordWriter {
        private final RecordId stableId;

        private NodeStateWriter(RecordId stableId, List<RecordId> ids) {
            super(NODE, RECORD_ID_BYTES, ids);
            this.stableId = stableId;
        }

        @Override
        protected RecordId writeRecordContent(RecordId id, SegmentBufferWriter writer) {

            // Write the stable record ID. If no stable ID exists (in case of a
            // new node state), it is generated from the current record ID. In
            // this case, the generated stable ID is only a marker and is not a
            // reference to another record.

            if (stableId == null) {
                // Write this node's record id to indicate that the stable id is not
                // explicitly stored.
                writer.writeRecordId(id);
            } else {
                writer.writeRecordId(stableId);
            }

            for (RecordId recordId : ids) {
                writer.writeRecordId(recordId);
            }
            return id;
        }
    }

}
