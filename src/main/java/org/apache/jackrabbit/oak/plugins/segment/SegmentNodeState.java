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
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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

public class SegmentNodeState extends Record implements NodeState {

    private volatile RecordId templateId = null;

    private volatile Template template = null;

    public SegmentNodeState(Segment segment, RecordId id) {
        super(segment, id);
    }

    RecordId getTemplateId() {
        if (templateId == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            templateId = getSegment().readRecordId(getOffset(0));
        }
        return templateId;
    }

    Template getTemplate() {
        if (template == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            template = getSegment().readTemplate(getTemplateId());
        }
        return template;
    }

    MapRecord getChildNodeMap() {
        Segment segment = getSegment();
        return segment.readMap(segment.readRecordId(getOffset(0, 1)));
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
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
    public boolean hasProperty(String name) {
        checkNotNull(name);
        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            return template.getPrimaryType() != null;
        } else if (JCR_MIXINTYPES.equals(name)) {
            return template.getMixinTypes() != null;
        } else {
            return template.getPropertyTemplate(name) != null;
        }
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        checkNotNull(name);
        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            return template.getPrimaryType();
        } else if (JCR_MIXINTYPES.equals(name)) {
            return template.getMixinTypes();
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate != null) {
            Segment segment = getSegment();
            int ids = 1 + propertyTemplate.getIndex();
            if (template.getChildName() != Template.ZERO_CHILD_NODES) {
                ids++;
            }
            return new SegmentPropertyState(
                    segment, segment.readRecordId(getOffset(0, ids)),
                    propertyTemplate);
        } else {
            return null;
        }
    }

    @Override @Nonnull
    public Iterable<PropertyState> getProperties() {
        Template template = getTemplate();
        PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
        List<PropertyState> list =
                newArrayListWithCapacity(propertyTemplates.length + 2);

        PropertyState primaryType = template.getPrimaryType();
        if (primaryType != null) {
            list.add(primaryType);
        }

        PropertyState mixinTypes = template.getMixinTypes();
        if (mixinTypes != null) {
            list.add(mixinTypes);
        }

        Segment segment = getSegment();
        int ids = 1;
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        for (int i = 0; i < propertyTemplates.length; i++) {
            RecordId propertyId = segment.readRecordId(getOffset(0, ids++));
            list.add(new SegmentPropertyState(
                    segment, propertyId, propertyTemplates[i]));
        }

        return list;
    }

    @Override
    public boolean getBoolean(String name) {
        return Boolean.TRUE.toString().equals(getValueAsString(name, BOOLEAN));
    }

    @Override
    public long getLong(String name) {
        String value = getValueAsString(name, LONG);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return 0;
        }
    }

    @Override @CheckForNull
    public String getString(String name) {
        return getValueAsString(name, STRING);
    }

    @Override @Nonnull
    public Iterable<String> getStrings(String name) {
        return getValuesAsStrings(name, STRINGS);
    }

    @Override @CheckForNull
    public String getName(String name) {
        return getValueAsString(name, NAME);
    }

    @Override @Nonnull
    public Iterable<String> getNames(String name) {
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
    @CheckForNull
    private String getValueAsString(String name, Type<?> type) {
        checkArgument(!type.isArray());

        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            PropertyState primary = template.getPrimaryType();
            if (type == NAME) {
                return primary.getValue(NAME);
            } else if (primary != null) {
                return null;
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
        int ids = 1 + propertyTemplate.getIndex();
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        return segment.readString(segment.readRecordId(getOffset(0, ids)));
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
    @Nonnull
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
        int ids = 1 + propertyTemplate.getIndex();
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }

        RecordId id = segment.readRecordId(getOffset(0, ids));
        segment = segment.getSegment(id);
        int size = segment.readInt(id.getOffset());
        if (size == 0) {
            return emptyList();
        }

        id = segment.readRecordId(id.getOffset() + 4);
        if (size == 1) {
            return singletonList(segment.readString(id));
        }

        List<String> values = newArrayListWithCapacity(size);
        ListRecord list = new ListRecord(segment, id, size);
        for (RecordId value : list.getEntries()) {
            values.add(segment.readString(value));
        }
        return values;
    }

    @Override
    public long getChildNodeCount(long max) {
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
    public boolean hasChildNode(String name) {
        checkArgument(!checkNotNull(name).isEmpty());
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return false;
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getEntry(name) != null;
        } else {
            return childName.equals(name);
        }
    }

    @Override @Nonnull
    public NodeState getChildNode(String name) {
        // checkArgument(!checkNotNull(name).isEmpty()); // TODO
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return MISSING_NODE;
        } else if (childName == Template.MANY_CHILD_NODES) {
            MapEntry child = getChildNodeMap().getEntry(name);
            if (child != null) {
                return child.getNodeState();
            } else {
                return MISSING_NODE;
            }
        } else {
            if (childName.equals(name)) {
                Segment segment = getSegment();
                RecordId childNodeId = segment.readRecordId(getOffset(0, 1));
                return new SegmentNodeState(segment, childNodeId);
            } else {
                return MISSING_NODE;
            }
        }
    }

    @Override @Nonnull
    public Iterable<String> getChildNodeNames() {
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getKeys();
        } else {
            return Collections.singletonList(childName);
        }
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        String childName = getTemplate().getChildName();
        if (childName == Template.ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == Template.MANY_CHILD_NODES) {
            return getChildNodeMap().getEntries();
        } else {
            Segment segment = getSegment();
            RecordId childNodeId = segment.readRecordId(getOffset(0, 1));
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, new SegmentNodeState(segment, childNodeId)));
        }
    }

    @Override @Nonnull
    public SegmentNodeBuilder builder() {
        return new SegmentNodeBuilder(this);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base || fastEquals(this, base)) {
             return true; // no changes
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return getTemplate().compareAgainstEmptyState(
                    getSegment(), getRecordId(), diff);
        } else if (base instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) base;
            return getTemplate().compareAgainstBaseState(
                    getSegment(), getRecordId(), that.getTemplate(),
                    that.getSegment(), that.getRecordId(), diff);
        } else {
            // fallback
            return AbstractNodeState.compareAgainstBaseState(this, base, diff);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object || fastEquals(this, object)) {
            return true;
        } else if (object instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) object;
            Template template = getTemplate();
            return template.equals(that.getTemplate())
                    && template.compare(
                            getSegment(), getRecordId(),
                            that.getSegment(), that.getRecordId());
        } else {
            return object instanceof NodeState
                    && AbstractNodeState.equals(this, (NodeState) object); // TODO
        }
    }

    @Override
    public String toString() {
        return AbstractNodeState.toString(this);
    }

}
