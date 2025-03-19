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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.guava.common.base.Suppliers.memoize;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.jackrabbit.guava.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A record of type "NODE". This class can read a node record from a segment. It
 * currently doesn't cache data (but the template is fully loaded).
 */
public class SegmentNodeState extends Record implements NodeState {
    @NotNull
    private final SegmentReader reader;

    @Nullable
    private final BlobStore blobStore;

    @NotNull
    private final Supplier<SegmentWriter> writer;

    private final MeterStats readStats;

    private volatile RecordId templateId = null;

    private volatile Template template = null;

    SegmentNodeState(
        @NotNull SegmentReader reader,
        @NotNull Supplier<SegmentWriter> writer,
        @Nullable BlobStore blobStore,
        @NotNull RecordId id,
        MeterStats readStats
    ) {
        super(id);
        this.reader = requireNonNull(reader);
        this.writer = requireNonNull(memoize(writer::get));
        this.blobStore = blobStore;
        this.readStats = readStats;
    }

    public SegmentNodeState(
        @NotNull SegmentReader reader,
        @NotNull SegmentWriter writer,
        @Nullable BlobStore blobStore,
        @NotNull RecordId id
    ) {
        this(reader, Suppliers.ofInstance(writer), blobStore, id, NoopStats.INSTANCE);
    }

    public SegmentNodeState(
        @NotNull SegmentReader reader,
        @NotNull SegmentWriter writer,
        @Nullable BlobStore blobStore,
        @NotNull RecordId id,
        MeterStats readStats
    ) {
        this(reader, Suppliers.ofInstance(writer), blobStore, id, readStats);
    }

    RecordId getTemplateId() {
        if (templateId == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            templateId = getSegment().readRecordId(getRecordNumber(), 0, 1);
        }
        return templateId;
    }

    Template getTemplate() {
        if (template == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            template = reader.readTemplate(getTemplateId());
        }
        return template;
    }

    MapRecord getChildNodeMap() {
        Segment segment = getSegment();
        return reader.readMap(segment.readRecordId(getRecordNumber(), 0, 2));
    }

    @NotNull
    static String getStableId(@NotNull Buffer stableId) {
        Buffer buffer = stableId.duplicate();
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        int offset = buffer.getInt();
        return new UUID(msb, lsb) + ":" + offset;
    }

    /**
     * Returns the stable id of this node. In contrast to the node's record id
     * (which is technically the node's address) the stable id doesn't change
     * after an online gc cycle. It might though change after an offline gc cycle.
     *
     * @return  stable id
     */
    public String getStableId() {
        return getStableId(getStableIdBytes());
    }

    /**
     * Returns the stable ID of this node, non parsed. In contrast to the node's
     * record id (which is technically the node's address) the stable id doesn't
     * change after an online gc cycle. It might though change after an offline
     * gc cycle.
     *
     * @return the stable ID of this node.
     */
    public Buffer getStableIdBytes() {
        // The first record id of this node points to the stable id.
        RecordId id = getSegment().readRecordId(getRecordNumber());

        if (id.equals(getRecordId())) {
            // If that id is equal to the record id of this node then the stable
            // id is the string representation of the record id of this node.
            // See RecordWriters.NodeStateWriter.writeRecordContent()
            return id.getBytes();
        } else {
            // Otherwise that id points to the serialised (msb, lsb, offset)
            // stable id.
            return id.getSegment().readBytes(id.getRecordNumber(), 0, RecordId.SERIALIZED_RECORD_ID_BYTES);
        }
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
        readStats.mark();
        Template template = getTemplate();
        long count = template.getPropertyTemplates().length;
        if (template.getPrimaryType() != null) {
            count++;
        }
        if (template.getMixinTypes() != null) {
            count++;
        }
        return count;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        readStats.mark();
        requireNonNull(name);
        Template template = getTemplate();
        switch (name) {
            case JCR_PRIMARYTYPE:
                return template.getPrimaryType() != null;
            case JCR_MIXINTYPES:
                return template.getMixinTypes() != null;
            default:
                return template.getPropertyTemplate(name) != null;
        }
    }

    @Override @Nullable
    public PropertyState getProperty(@NotNull String name) {
        readStats.mark();
        requireNonNull(name);
        Template template = getTemplate();
        PropertyState property = null;
        if (JCR_PRIMARYTYPE.equals(name)) {
            property = template.getPrimaryType();
        } else if (JCR_MIXINTYPES.equals(name)) {
            property = template.getMixinTypes();
        }
        if (property != null) {
            return property;
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate != null) {
            Segment segment = getSegment();
            RecordId id = getRecordId(segment, template, propertyTemplate);
            return reader.readProperty(id, propertyTemplate);
        } else {
            return null;
        }
    }

    private RecordId getRecordId(Segment segment, Template template,
                                 PropertyTemplate propertyTemplate) {
        int ids = 2;
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        RecordId rid = segment.readRecordId(getRecordNumber(), 0, ids);
        ListRecord pIds = new ListRecord(rid, template.getPropertyTemplates().length);
        return pIds.getEntry(propertyTemplate.getIndex());
    }

    @Override @NotNull
    public Iterable<PropertyState> getProperties() {
        readStats.mark();
        Template template = getTemplate();
        PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
        List<PropertyState> list = new ArrayList<>(propertyTemplates.length + 2);

        PropertyState primaryType = template.getPrimaryType();
        if (primaryType != null) {
            list.add(primaryType);
        }

        PropertyState mixinTypes = template.getMixinTypes();
        if (mixinTypes != null) {
            list.add(mixinTypes);
        }

        Segment segment = getSegment();
        int ids = 2;
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }

        if (propertyTemplates.length > 0) {
            ListRecord pIds = new ListRecord(segment.readRecordId(getRecordNumber(), 0, ids), propertyTemplates.length);
            for (int i = 0; i < propertyTemplates.length; i++) {
                RecordId propertyId = pIds.getEntry(i);
                list.add(reader.readProperty(propertyId, propertyTemplates[i]));
            }
        }

        return list;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        readStats.mark();
        return Boolean.TRUE.toString().equals(getValueAsString(name, BOOLEAN));
    }

    @Override
    public long getLong(String name) {
        readStats.mark();
        String value = getValueAsString(name, LONG);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return 0;
        }
    }

    @Override @Nullable
    public String getString(String name) {
        readStats.mark();
        return getValueAsString(name, STRING);
    }

    @Override @NotNull
    public Iterable<String> getStrings(@NotNull String name) {
        readStats.mark();
        return getValuesAsStrings(name, STRINGS);
    }

    @Override @Nullable
    public String getName(@NotNull String name) {
        readStats.mark();
        return getValueAsString(name, NAME);
    }

    @Override @NotNull
    public Iterable<String> getNames(@NotNull String name) {
        readStats.mark();
        return getValuesAsStrings(name, NAMES);
    }

    /**
     * Optimized value access method. Returns the string value of a property
     * of a given non-array type. Returns {@code null} if the named property
     * does not exist, or is of a different type than given.
     *
     * @param name property name
     * @param type property type
     * @return string value of the property, or {@code null}
     */
    @Nullable
    private String getValueAsString(String name, Type<?> type) {
        checkArgument(!type.isArray());

        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            PropertyState primary = template.getPrimaryType();
            if (primary != null) {
                if (type == NAME) {
                    return primary.getValue(NAME);
                } else {
                    return null;
                }
            }
        } else if (JCR_MIXINTYPES.equals(name)
                && template.getMixinTypes() != null) {
            return null;
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate == null
                || propertyTemplate.getType() != type) {
            return null;
        }

        Segment segment = getSegment();
        RecordId id = getRecordId(segment, template, propertyTemplate);
        return reader.readString(id);
    }

    /**
     * Optimized value access method. Returns the string values of a property
     * of a given array type. Returns an empty iterable if the named property
     * does not exist, or is of a different type than given.
     *
     * @param name property name
     * @param type property type
     * @return string values of the property, or an empty iterable
     */
    @NotNull
    private Iterable<String> getValuesAsStrings(String name, Type<?> type) {
        checkArgument(type.isArray());

        Template template = getTemplate();
        if (JCR_MIXINTYPES.equals(name)) {
            PropertyState mixin = template.getMixinTypes();
            if (type == NAMES && mixin != null) {
                return mixin.getValue(NAMES);
            } else if (type == NAMES || mixin != null) {
                return emptyList();
            }
        } else if (JCR_PRIMARYTYPE.equals(name)
                && template.getPrimaryType() != null) {
            return emptyList();
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate == null
                || propertyTemplate.getType() != type) {
            return emptyList();
        }

        Segment segment = getSegment();
        RecordId id = getRecordId(segment, template, propertyTemplate);
        segment = id.getSegment();
        int size = segment.readInt(id.getRecordNumber());
        if (size == 0) {
            return emptyList();
        }

        id = segment.readRecordId(id.getRecordNumber(), 4);
        if (size == 1) {
            return singletonList(reader.readString(id));
        }

        List<String> values = new ArrayList<>(size);
        ListRecord list = new ListRecord(id, size);
        for (RecordId value : list.getEntries()) {
            values.add(reader.readString(value));
        }
        return values;
    }

    @Override
    public long getChildNodeCount(long max) {
        readStats.mark();
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return 0;
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().size();
        } else {
            return 1;
        }
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        readStats.mark();
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return false;
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getEntry(name) != null;
        } else {
            return childName.equals(name);
        }
    }

    @Override @NotNull
    public NodeState getChildNode(@NotNull String name) {
        readStats.mark();
        String childName = getTemplate().getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            MapEntry child = getChildNodeMap().getEntry(name);
            if (child != null) {
                return child.getNodeState();
            }
        } else if (childName != Template.ZERO_CHILD_NODES
                && childName.equals(name)) {
            RecordId childNodeId = getSegment().readRecordId(getRecordNumber(), 0, 2);
            return reader.readNode(childNodeId);
        }
        checkValidName(name);
        return MISSING_NODE;
    }

    @Override @NotNull
    public Iterable<String> getChildNodeNames() {
        readStats.mark();
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getKeys();
        } else {
            return Collections.singletonList(childName);
        }
    }

    @Override @NotNull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        readStats.mark();
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getEntries();
        } else {
            RecordId childNodeId = getSegment().readRecordId(getRecordNumber(), 0, 2);
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, reader.readNode(childNodeId)));
        }
    }

    @Override @NotNull
    public SegmentNodeBuilder builder() {
        return new SegmentNodeBuilder(this, blobStore, reader, writer.get(), readStats);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        readStats.mark();
        if (this == base || fastEquals(this, base)) {
             return true; // no changes
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (!(base instanceof SegmentNodeState)) { // fallback
            return AbstractNodeState.compareAgainstBaseState(this, base, diff);
        }

        SegmentNodeState that = (SegmentNodeState) base;

        Template beforeTemplate = that.getTemplate();
        RecordId beforeId = that.getRecordId();

        Template afterTemplate = getTemplate();
        RecordId afterId = getRecordId();

        // Compare type properties
        if (!compareProperties(
                beforeTemplate.getPrimaryType(), afterTemplate.getPrimaryType(),
                diff)) {
            return false;
        }
        if (!compareProperties(
                beforeTemplate.getMixinTypes(), afterTemplate.getMixinTypes(),
                diff)) {
            return false;
        }

        // Compare other properties, leveraging the ordering
        int beforeIndex = 0;
        int afterIndex = 0;
        PropertyTemplate[] beforeProperties =
                beforeTemplate.getPropertyTemplates();
        PropertyTemplate[] afterProperties =
                afterTemplate.getPropertyTemplates();
        while (beforeIndex < beforeProperties.length
                && afterIndex < afterProperties.length) {
            int d = Integer.valueOf(afterProperties[afterIndex].hashCode())
                    .compareTo(beforeProperties[beforeIndex].hashCode());
            if (d == 0) {
                d = afterProperties[afterIndex].getName().compareTo(
                        beforeProperties[beforeIndex].getName());
            }
            PropertyState beforeProperty = null;
            PropertyState afterProperty = null;
            if (d < 0) {
                afterProperty =
                        afterTemplate.getProperty(afterId, afterIndex++);
            } else if (d > 0) {
                beforeProperty =
                        beforeTemplate.getProperty(beforeId, beforeIndex++);
            } else {
                afterProperty =
                        afterTemplate.getProperty(afterId, afterIndex++);
                beforeProperty =
                        beforeTemplate.getProperty(beforeId, beforeIndex++);
            }
            if (!compareProperties(beforeProperty, afterProperty, diff)) {
                return false;
            }
        }
        while (afterIndex < afterProperties.length) {
            if (!diff.propertyAdded(
                    afterTemplate.getProperty(afterId, afterIndex++))) {
                return false;
            }
        }
        while (beforeIndex < beforeProperties.length) {
            PropertyState beforeProperty =
                    beforeTemplate.getProperty(beforeId, beforeIndex++);
            if (!diff.propertyDeleted(beforeProperty)) {
                return false;
            }
        }

        String beforeChildName = beforeTemplate.getChildName();
        String afterChildName = afterTemplate.getChildName();
        if (afterChildName == Template.ZERO_CHILD_NODES) {
            if (beforeChildName != Template.ZERO_CHILD_NODES) {
                for (ChildNodeEntry entry
                        : beforeTemplate.getChildNodeEntries(beforeId)) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            }
        } else if (afterChildName != Template.MANY_CHILD_NODES) {
            NodeState afterNode =
                    afterTemplate.getChildNode(afterChildName, afterId);
            NodeState beforeNode =
                    beforeTemplate.getChildNode(afterChildName, beforeId);
            if (!beforeNode.exists()) {
                if (!diff.childNodeAdded(afterChildName, afterNode)) {
                    return false;
                }
            } else if (!fastEquals(afterNode, beforeNode)) {
                if (!diff.childNodeChanged(
                        afterChildName, beforeNode, afterNode)) {
                    return false;
                }
            }
            if (beforeChildName == Template.MANY_CHILD_NODES
                    || (beforeChildName != Template.ZERO_CHILD_NODES
                        && !beforeNode.exists())) {
                for (ChildNodeEntry entry
                        : beforeTemplate.getChildNodeEntries(beforeId)) {
                    if (!afterChildName.equals(entry.getName())) {
                        if (!diff.childNodeDeleted(
                                entry.getName(), entry.getNodeState())) {
                            return false;
                        }
                    }
                }
            }
        } else if (beforeChildName == Template.ZERO_CHILD_NODES) {
            for (ChildNodeEntry entry
                    : afterTemplate.getChildNodeEntries(afterId)) {
                if (!diff.childNodeAdded(
                        entry.getName(), entry.getNodeState())) {
                    return false;
                }
            }
        } else if (beforeChildName != Template.MANY_CHILD_NODES) {
            boolean beforeChildRemoved = true;
            NodeState beforeChild =
                    beforeTemplate.getChildNode(beforeChildName, beforeId);
            for (ChildNodeEntry entry
                    : afterTemplate.getChildNodeEntries(afterId)) {
                String childName = entry.getName();
                NodeState afterChild = entry.getNodeState();
                if (beforeChildName.equals(childName)) {
                    beforeChildRemoved = false;
                    if (!fastEquals(afterChild, beforeChild)
                            && !diff.childNodeChanged(
                                    childName, beforeChild, afterChild)) {
                        return false;
                    }
                } else if (!diff.childNodeAdded(childName, afterChild)) {
                    return false;
                }
            }
            if (beforeChildRemoved) {
                if (!diff.childNodeDeleted(beforeChildName, beforeChild)) {
                    return false;
                }
            }
        } else {
            MapRecord afterMap = afterTemplate.getChildNodeMap(afterId);
            MapRecord beforeMap = beforeTemplate.getChildNodeMap(beforeId);
            return afterMap.compare(beforeMap, diff);
        }

        return true;
    }

    private static boolean compareProperties(
            PropertyState before, PropertyState after, NodeStateDiff diff) {
        if (before == null) {
            return after == null || diff.propertyAdded(after);
        } else if (after == null) {
            return diff.propertyDeleted(before);
        } else {
            return before.equals(after) || diff.propertyChanged(before, after);
        }
    }

    //------------------------------------------------------------< Object >--

    /**
     * Indicates whether two {@link NodeState} instances are equal to each
     * other. A return value of {@code true} clearly means that the instances
     * are equal, while a return value of {@code false} doesn't necessarily mean
     * the instances are not equal. These "false negatives" are an
     * implementation detail and callers cannot rely on them being stable.
     *
     * @param a
     *            the first {@link NodeState} instance
     * @param b
     *            the second {@link NodeState} instance
     * @return {@code true}, if these two instances are equal.
     */
    public static boolean fastEquals(NodeState a, NodeState b) {
        if (Record.fastEquals(a, b)) {
            return true;
        }

        if (a instanceof SegmentNodeState && b instanceof SegmentNodeState
            && ((SegmentNodeState) a).getStableId().equals(((SegmentNodeState) b).getStableId())) {
                return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return getStableId().hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) object;
            if (fastEquals(this, that)) {
                return true;
            } else {
                Template template = getTemplate();
                return template.equals(that.getTemplate())
                    && template.compare(getRecordId(), that.getRecordId());
            }
        } else {
            return object instanceof NodeState
                    && AbstractNodeState.equals(this, (NodeState) object);
        }
    }

    @Override
    public String toString() {
        return AbstractNodeState.toString(this);
    }

}
