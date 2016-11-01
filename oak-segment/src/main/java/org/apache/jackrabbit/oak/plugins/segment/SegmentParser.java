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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.segment.ListRecord.LEVEL_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Template.MANY_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.segment.Template.ZERO_CHILD_NODES;

import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code SegmentParser} serves as a base class for parsing segments.
 * <p>
 * This base class provides means for parsing segments into their various
 * kinds of record. Descendants typically parametrise its behaviour by
 * overriding the {@code on...()} methods as needed. By default those
 * methods just initiate the traversal of the same named record.
 * <p>
 * A typical usage for e.g. printing out the sizes of all templates
 * would look as follows:
 * <pre>
      new TestParser() {
          protected void onTemplate(RecordId parentId, RecordId templateId) {
              TemplateInfo templateInfo = parseTemplate(parentId, templateId);
              System.out.println(templateInfo.size);
          }
     }.parseNode(null, nodeId);
 * </pre>
 */
@Deprecated
public class SegmentParser {

    /**
     * Type of blobs (and strings)
     */
    @Deprecated
    public enum BlobType {
        /** Small: &lt; {@link Segment#SMALL_LIMIT}. */
        @Deprecated
        SMALL,

        /** Medium: &lt; {@link Segment#MEDIUM_LIMIT} */
        @Deprecated
        MEDIUM,

        /** Long: &gt;=  {@link Segment#MEDIUM_LIMIT} */
        @Deprecated
        LONG,

        /** External blob (i.e. in {@link BlobStore}. */
        @Deprecated
        EXTERNAL
    }

    /**
     * Result type of {@link #parseNode(RecordId)}.
     */
    @Deprecated
    public static class NodeInfo {
        /** Id of this record*/
        @Deprecated
        public final RecordId nodeId;

        /** Number of child nodes */
        @Deprecated
        public final int nodeCount;

        /** Number of properties */
        @Deprecated
        public final int propertyCount;

        /** Size in bytes of this node */
        @Deprecated
        public final int size;

        @Deprecated
        public NodeInfo(RecordId nodeId, int nodeCount, int propertyCount, int size) {
            this.nodeId = nodeId;
            this.nodeCount = nodeCount;
            this.propertyCount = propertyCount;
            this.size = size;
        }
    }

    /**
     * Result type of {@link #parseTemplate(RecordId)}.
     */
    @Deprecated
    public static class TemplateInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId templateId;

        /** Nodes of this type have a primary type */
        @Deprecated
        public final boolean hasPrimaryType;

        /** Nodes of this type have mixins */
        @Deprecated
        public final boolean hasMixinType;

        /** Nodes with this type have no child nodes */
        @Deprecated
        public final boolean zeroChildNodes;

        /** Nodes of this type have more than one child node */
        @Deprecated
        public final boolean manyChildNodes;

        /** Number of mixins */
        @Deprecated
        public final int mixinCount;

        /** Number of properties */
        @Deprecated
        public final int propertyCount;

        /** Size in bytes of this template */
        @Deprecated
        public final int size;

        @Deprecated
        public TemplateInfo(RecordId templateId, boolean hasPrimaryType, boolean hasMixinType,
                boolean zeroChildNodes, boolean manyChildNodes, int mixinCount, int propertyCount, int size) {
            this.templateId = templateId;
            this.hasPrimaryType = hasPrimaryType;
            this.hasMixinType = hasMixinType;
            this.zeroChildNodes = zeroChildNodes;
            this.manyChildNodes = manyChildNodes;
            this.mixinCount = mixinCount;
            this.propertyCount = propertyCount;
            this.size = size;
        }
    }

    /**
     * Result type of {@link #parseMap(RecordId, RecordId, MapRecord)}.
     */
    @Deprecated
    public static class MapInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId mapId;

        /** Size in bytes of this map. {@code -1} if not known. */
        @Deprecated
        public final int size;

        @Deprecated
        public MapInfo(RecordId mapId, int size) {
            this.mapId = mapId;
            this.size = size;
        }
    }

    /**
     * Result type of {@link #parseProperty(RecordId, RecordId, PropertyTemplate)}.
     */
    @Deprecated
    public static class PropertyInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId propertyId;

        /** Number of values in properties of this type. {@code -1} for single value properties. */
        @Deprecated
        public final int count;

        /** Size in bytes of this property */
        @Deprecated
        public final int size;

        @Deprecated
        public PropertyInfo(RecordId propertyId, int count, int size) {
            this.propertyId = propertyId;
            this.count = count;
            this.size = size;
        }
    }

    /** Result type of {@link #parseValue(RecordId, RecordId, Type)}. */
    @Deprecated
    public static class ValueInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId valueId;

        /** Type of this value */
        @Deprecated
        public final Type<?> type;

        @Deprecated
        public ValueInfo(RecordId valueId, Type<?> type) {
            this.valueId = valueId;
            this.type = type;
        }
    }

    /** Return type of {@link #parseBlob(RecordId)}. */
    @Deprecated
    public static class BlobInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId blobId;

        /** Type of this blob */
        @Deprecated
        public final BlobType blobType;

        /** Size in bytes of this blob */
        @Deprecated
        public final int size;

        @Deprecated
        public BlobInfo(RecordId blobId, BlobType blobType, int size) {
            this.blobId = blobId;
            this.blobType = blobType;
            this.size = size;
        }
    }

    /** Return type of {@link #parseList(RecordId, RecordId, int)} . */
    @Deprecated
    public static class ListInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId listId;

        /** Number of items in this list */
        @Deprecated
        public final int count;

        /** Size in bytes of this list */
        @Deprecated
        public final int size;

        @Deprecated
        public ListInfo(RecordId listId, int count, int size) {
            this.listId = listId;
            this.count = count;
            this.size = size;
        }
    }

    /** Return type of {@link #parseListBucket(RecordId, int, int, int)}. */
    @Deprecated
    public static class ListBucketInfo {
        /** Id of this record */
        @Deprecated
        public final RecordId listId;

        /** {@code true} if this is a leaf bucket, {@code false} otherwise. */
        @Deprecated
        public final boolean leaf;

        /** Entries of this bucket */
        @Deprecated
        public final List<RecordId> entries;

        /** Size in bytes of this bucket. */
        @Deprecated
        public final int size;

        @Deprecated
        public ListBucketInfo(RecordId listId, boolean leaf, List<RecordId> entries, int size) {
            this.listId = listId;
            this.leaf = leaf;
            this.entries = entries;
            this.size = size;
        }
    }

    /**
     * Callback called by {@link #parseNode(RecordId)} upon encountering
     * a child node.
     *
     * @param parentId  id of the parent node
     * @param nodeId    if of the child node
     */
    @Deprecated
    protected void onNode(RecordId parentId, RecordId nodeId) {
        parseNode(nodeId);
    }

    /**
     * Callback called by {@link #parseNode(RecordId)} upon encountering
     * a template
     *
     * @param parentId   id of the node being parsed
     * @param templateId id of the template
     */
    @Deprecated
    protected void onTemplate(RecordId parentId, RecordId templateId) {
        parseTemplate(templateId);
    }

    /**
     * Callback called by {@link #parseNode(RecordId)},
     * {@link #parseMapDiff(RecordId, MapRecord)} and
     * {@link #parseMapBranch(RecordId, MapRecord)} upon encountering a map.
     *
     * @param parentId  the id of the parent of the map
     * @param mapId     the id of the map
     * @param map       the map
     */
    @Deprecated
    protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
        parseMap(parentId, mapId, map);
    }

    /**
     * Callback called by {@link #parseMap(RecordId, RecordId, MapRecord)} upon encountering
     * a map diff.
     *
     * @param parentId  the id of the parent map
     * @param mapId     the id of the map
     * @param map       the map
     */
    @Deprecated
    protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
        parseMapDiff(mapId, map);
    }

    /**
     * Callback called by {@link #parseMap(RecordId, RecordId, MapRecord)} upon encountering
     * a map leaf.
     *
     * @param parentId  the id of the parent map
     * @param mapId     the id of the map
     * @param map       the map
     */
    @Deprecated
    protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
        parseMapLeaf(mapId, map);
    }

    /**
     * Callback called by {@link #parseMap(RecordId, RecordId, MapRecord)} upon encountering
     * a map branch.
     *
     * @param parentId  the id of the parent map
     * @param mapId     the id of the map
     * @param map       the map
     */
    @Deprecated
    protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
        parseMapBranch(mapId, map);
    }

    /**
     * Callback called by {@link #parseNode(RecordId)} upon encountering
     * a property.
     *
     * @param parentId    the id of the parent node
     * @param propertyId  the id of the property
     * @param template    the property template
     */
    @Deprecated
    protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
        parseProperty(parentId, propertyId, template);
    }

    /**
     * Callback called by {@link #parseProperty(RecordId, RecordId, PropertyTemplate)} upon
     * encountering a value.
     *
     * @param parentId   the id the value's parent
     * @param valueId    the id of the value
     * @param type       the type of the value
     */
    @Deprecated
    protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) {
        parseValue(parentId, valueId, type);
    }

    /**
     * Callback called by {@link #parseValue(RecordId, RecordId, Type)} upon encountering a blob.
     *
     * @param parentId  the id of the blob's parent
     * @param blobId    the id of the blob
     */
    @Deprecated
    protected void onBlob(RecordId parentId, RecordId blobId) {
        parseBlob(blobId);
    }

    /**
     * Callback called by {@link #parseTemplate(RecordId)},
     * {@link #parseMapLeaf(RecordId, MapRecord)} and
     * {@link #parseValue(RecordId, RecordId, Type)} upon encountering a string.
     *
     * @param parentId  the id of the string's parent
     * @param stringId  the id of the string
     */
    @Deprecated
    protected void onString(RecordId parentId, RecordId stringId) {
        parseString(stringId);
    }

    /**
     * Callback called by {@link #parseNode(RecordId)},
     * {@link #parseProperty(RecordId, RecordId, PropertyTemplate)},
     * {@link #parseTemplate(RecordId)},
     * {@link #parseBlob(RecordId)} and
     * {@link #parseString(RecordId)} upon encountering a list.
     *
     * @param parentId  the id of the list's parent
     * @param listId    the id of the list
     * @param count     the number of elements in the list
     */
    @Deprecated
    protected void onList(RecordId parentId, RecordId listId, int count) {
        parseList(parentId, listId, count);
    }

    /**
     * Callback called by {@link #parseList(RecordId, RecordId, int)} and
     * {@link #parseListBucket(RecordId, int, int, int)} upon encountering a list
     * bucket.
     *
     * @param parentId    the id of the list's parent
     * @param listId      the id of the list
     * @param index       the index into the bucket
     * @param count       the number of items in the bucket
     * @param capacity    the capacity of the bucket
     */
    @Deprecated
    protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
        parseListBucket(listId, index, count, capacity);
    }

    /**
     * Parse a node record
     * @param nodeId
     * @return
     */
    @Deprecated
    public NodeInfo parseNode(RecordId nodeId) {
        int size = 0;
        int nodeCount = 0;
        int propertyCount = 0;

        Segment segment = nodeId.getSegment();
        int offset = nodeId.getOffset();
        RecordId templateId = segment.readRecordId(offset);
        onTemplate(nodeId, templateId);

        Template template = segment.readTemplate(templateId);

        // Recurses into child nodes in this segment
        if (template.getChildName() == MANY_CHILD_NODES) {
            RecordId childMapId = segment.readRecordId(offset + RECORD_ID_BYTES);
            MapRecord childMap = segment.readMap(childMapId);
            onMap(nodeId, childMapId, childMap);
            for (ChildNodeEntry childNodeEntry : childMap.getEntries()) {
                NodeState child = childNodeEntry.getNodeState();
                if (child instanceof SegmentNodeState) {
                    RecordId childId = ((SegmentNodeState) child).getRecordId();
                    onNode(nodeId, childId);
                    nodeCount++;
                }
            }
        } else if (template.getChildName() != ZERO_CHILD_NODES) {
            RecordId childId = segment.readRecordId(offset + RECORD_ID_BYTES);
            onNode(nodeId, childId);
            nodeCount++;
        }

        int ids = template.getChildName() == ZERO_CHILD_NODES ? 1 : 2;
        size += ids * RECORD_ID_BYTES;

        // Recurse into properties
        PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
        if (segment.getSegmentVersion().onOrAfter(V_11)) {
            if (propertyTemplates.length > 0) {
                size += RECORD_ID_BYTES;
                RecordId id = segment.readRecordId(offset + ids * RECORD_ID_BYTES);
                ListRecord pIds = new ListRecord(id,
                        propertyTemplates.length);
                for (int i = 0; i < propertyTemplates.length; i++) {
                    RecordId propertyId = pIds.getEntry(i);
                    onProperty(nodeId, propertyId, propertyTemplates[i]);
                    propertyCount++;
                }
                onList(nodeId, id, propertyTemplates.length);
            }
        } else {
            for (PropertyTemplate propertyTemplate : propertyTemplates) {
                size += RECORD_ID_BYTES;
                RecordId propertyId = segment.readRecordId(offset + ids++ * RECORD_ID_BYTES);
                onProperty(nodeId, propertyId, propertyTemplate);
                propertyCount++;
            }
        }
        return new NodeInfo(nodeId, nodeCount, propertyCount, size);
    }

    /**
     * Parse a template record
     * @param templateId
     * @return
     */
    @Deprecated
    public TemplateInfo parseTemplate(RecordId templateId) {
        int size = 0;

        Segment segment = templateId.getSegment();
        int offset = templateId.getOffset();
        int head = segment.readInt(offset + size);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);
        size += 4;

        if (hasPrimaryType) {
            RecordId primaryId = segment.readRecordId(offset + size);
            onString(templateId, primaryId);
            size += RECORD_ID_BYTES;
        }

        if (hasMixinTypes) {
            for (int i = 0; i < mixinCount; i++) {
                RecordId mixinId = segment.readRecordId(offset + size);
                onString(templateId, mixinId);
                size += RECORD_ID_BYTES;
            }
        }

        if (!zeroChildNodes && !manyChildNodes) {
            RecordId childNameId = segment.readRecordId(offset + size);
            onString(templateId, childNameId);
            size += RECORD_ID_BYTES;
        }

        if (segment.getSegmentVersion().onOrAfter(V_11)) {
            if (propertyCount > 0) {
                RecordId listId = segment.readRecordId(offset + size);
                size += RECORD_ID_BYTES;
                ListRecord propertyNames = new ListRecord(listId, propertyCount);
                for (int i = 0; i < propertyCount; i++) {
                    RecordId propertyNameId = propertyNames.getEntry(i);
                    size++; // type
                    onString(templateId, propertyNameId);
                }
                onList(templateId, listId, propertyCount);
            }
        } else {
            for (int i = 0; i < propertyCount; i++) {
                RecordId propertyNameId = segment.readRecordId(offset + size);
                size += RECORD_ID_BYTES;
                size++;  // type
                onString(templateId, propertyNameId);
            }
        }

        return new TemplateInfo(templateId, hasPrimaryType, hasMixinTypes,
                zeroChildNodes, manyChildNodes, mixinCount, propertyCount, size);
    }

    /**
     * Parse a map record
     * @param parentId  parent of this map or {@code null} if none
     * @param mapId
     * @param map
     * @return
     */
    @Deprecated
    public MapInfo parseMap(RecordId parentId, RecordId mapId, MapRecord map) {
        if (map.isDiff()) {
            onMapDiff(parentId, mapId, map);
        } else if (map.isLeaf()) {
            onMapLeaf(parentId, mapId, map);
        } else {
            onMapBranch(parentId, mapId, map);
        }

        return new MapInfo(mapId, -1);
    }

    /**
     * Parse a map diff record
     * @param mapId
     * @param map
     * @return
     */
    @Deprecated
    public MapInfo parseMapDiff(RecordId mapId, MapRecord map) {
        int size = 4;                             // -1
        size += 4;                                // hash of changed key
        size += RECORD_ID_BYTES;                  // key
        size += RECORD_ID_BYTES;                  // value
        size += RECORD_ID_BYTES;                  // base

        RecordId baseId = mapId.getSegment()
                .readRecordId(mapId.getOffset() + 8 + 2 * RECORD_ID_BYTES);
        onMap(mapId, baseId, new MapRecord(baseId));

        return new MapInfo(mapId, size);
    }

    /**
     * Parse a map leaf record
     * @param mapId
     * @param map
     * @return
     */
    @Deprecated
    public MapInfo parseMapLeaf(RecordId mapId, MapRecord map) {
        int size = 4;                              // size
        size += map.size() * 4;                    // key hashes

        for (MapEntry entry : map.getEntries()) {
            size += 2 * RECORD_ID_BYTES;           // key value pairs
            onString(mapId, entry.getKey());
        }

        return new MapInfo(mapId, size);
    }

    /**
     * Parse a map branch record
     * @param mapId
     * @param map
     * @return
     */
    @Deprecated
    public MapInfo parseMapBranch(RecordId mapId, MapRecord map) {
        int size = 4;                                 // level/size
        size += 4;                                    // bitmap
        for (MapRecord bucket : map.getBuckets()) {
            if (bucket != null) {
                size += RECORD_ID_BYTES;
                onMap(map.getRecordId(), bucket.getRecordId(), bucket);
            }
        }

        return new MapInfo(mapId, size);
    }

    /**
     * Parse a property
     * @param parentId
     * @param propertyId
     * @param template
     * @return
     */
    @Deprecated
    public PropertyInfo parseProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
        int size = 0;
        int count = -1; // -1 -> single valued property

        Segment segment = propertyId.getSegment();
        int offset = propertyId.getOffset();
        Type<?> type = template.getType();

        if (type.isArray()) {
            count = segment.readInt(offset);
            size += 4;

            if (count > 0) {
                RecordId listId = segment.readRecordId(offset + 4);
                size += RECORD_ID_BYTES;
                for (RecordId valueId : new ListRecord(listId, count).getEntries()) {
                    onValue(propertyId, valueId, type.getBaseType());
                }
                onList(propertyId, listId, count);
            }
        } else {
            onValue(parentId, propertyId, type);
        }

        return new PropertyInfo(propertyId, count, size);
    }

    /**
     * Parse a value racrod
     * @param parentId  parent of the value record, {@code null} if none
     * @param valueId
     * @param type
     * @return
     */
    @Deprecated
    public ValueInfo parseValue(RecordId parentId, RecordId valueId, Type<?> type) {
        checkArgument(!type.isArray());
        if (type == BINARY) {
            onBlob(parentId, valueId);
        } else {
            onString(parentId, valueId);
        }
        return new ValueInfo(valueId, type);
    }

    /**
     * Parse a blob record
     * @param blobId
     * @return
     */
    @Deprecated
    public BlobInfo parseBlob(RecordId blobId) {
        int size = 0;
        BlobType blobType;

        Segment segment = blobId.getSegment();
        int offset = blobId.getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            size += (1 + head);
            blobType = BlobType.SMALL;
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            int length = (segment.readShort(offset) & 0x3fff) + SMALL_LIMIT;
            size += (2 + length);
            blobType = BlobType.MEDIUM;
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (segment.readLong(offset) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int count = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            RecordId listId = segment.readRecordId(offset + 8);
            onList(blobId, listId, count);
            size += (8 + RECORD_ID_BYTES + length);
            blobType = BlobType.LONG;
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            int length = (head & 0x0f) << 8 | (segment.readByte(offset + 1) & 0xff);
            size += (2 + length);
            blobType = BlobType.EXTERNAL;
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }

        return new BlobInfo(blobId, blobType, size);
    }

    /**
     * Parse a string record
     * @param stringId
     * @return
     */
    @Deprecated
    public BlobInfo parseString(RecordId stringId) {
        int size = 0;
        BlobType blobType;

        Segment segment = stringId.getSegment();
        int offset = stringId.getOffset();

        long length = segment.readLength(offset);
        if (length < Segment.SMALL_LIMIT) {
            size += (1 + length);
            blobType = BlobType.SMALL;
        } else if (length < Segment.MEDIUM_LIMIT) {
            size += (2 + length);
            blobType = BlobType.MEDIUM;
        } else if (length < Integer.MAX_VALUE) {
            int count = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            RecordId listId = segment.readRecordId(offset + 8);
            onList(stringId, listId, count);
            size += (8 + RECORD_ID_BYTES + length);
            blobType = BlobType.LONG;
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }

        return new BlobInfo(stringId, blobType, size);
    }

    /**
     * Parse a list record
     * @param parentId  parent of the list, {@code null} if none
     * @param listId
     * @param count
     * @return
     */
    @Deprecated
    public ListInfo parseList(RecordId parentId, RecordId listId, int count) {
        if (count != 0) {
            onListBucket(parentId, listId, 0, count, count);
        }

        return new ListInfo(listId, count, noOfListSlots(count) * RECORD_ID_BYTES);
    }

    /**
     * Parse item of list buckets
     * @param listId
     * @param index      index of the first item to parse
     * @param count      number of items to parse
     * @param capacity   total number of items
     * @return
     */
    @Deprecated
    public ListBucketInfo parseListBucket(RecordId listId, int index, int count, int capacity) {
        Segment segment = listId.getSegment();

        int bucketSize = 1;
        while (bucketSize * LEVEL_SIZE < capacity) {
            bucketSize *= LEVEL_SIZE;
        }

        List<RecordId> entries;
        if (capacity == 1) {
            entries = singletonList(listId);
            return new ListBucketInfo(listId, true, entries, RECORD_ID_BYTES);
        } else if (bucketSize == 1) {
            entries = newArrayListWithCapacity(count);
            for (int i = 0; i < count; i++) {
                entries.add(segment.readRecordId(getOffset(listId, index + i)));
            }
            return new ListBucketInfo(listId, true, entries, count * RECORD_ID_BYTES);
        } else {
            entries = newArrayList();
            while (count > 0) {
                int bucketIndex = index / bucketSize;
                int bucketOffset = index % bucketSize;
                RecordId bucketId = segment.readRecordId(getOffset(listId, bucketIndex));
                entries.add(bucketId);
                int c = Math.min(bucketSize, capacity - bucketIndex * bucketSize);
                int n = Math.min(c - bucketOffset, count);
                onListBucket(listId, bucketId, bucketOffset, n, c);

                index += n;
                count -= n;
            }
            return new ListBucketInfo(listId, false, entries, entries.size() * RECORD_ID_BYTES);
        }
    }

    private static int getOffset(RecordId id, int ids) {
        return id.getOffset() + ids * Segment.RECORD_ID_BYTES;
    }

    private static int noOfListSlots(int size) {
        if (size <= LEVEL_SIZE) {
            return size;
        } else {
            int fullBuckets = size / LEVEL_SIZE;
            if (size % LEVEL_SIZE > 1) {
                return size + noOfListSlots(fullBuckets + 1);
            } else {
                return size + noOfListSlots(fullBuckets);
            }
        }
    }

}
