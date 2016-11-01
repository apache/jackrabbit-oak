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
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A record of type "NODE". This class can read a node record from a segment. It
 * currently doesn't cache data (but the template is fully loaded).
 */
@Deprecated
public class SegmentNodeState extends Record implements NodeState {

    private volatile RecordId templateId = null;

    private volatile Template template = null;

    @Deprecated
    public SegmentNodeState(RecordId id) {
        super(id);
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
    @Deprecated
    public boolean exists() {
        return true;
    }

    @Override
    @Deprecated
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
    @Deprecated
    public boolean hasProperty(@Nonnull String name) {
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
    @Deprecated
    public PropertyState getProperty(@Nonnull String name) {
        checkNotNull(name);
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
            RecordId id;
            if (getSegment().getSegmentVersion().onOrAfter(V_11)) {
                id = getRecordIdV11(segment, template, propertyTemplate);
            } else {
                id = getRecordIdV10(segment, template, propertyTemplate);
            }
            return new SegmentPropertyState(id, propertyTemplate);
        } else {
            return null;
        }
    }

    private RecordId getRecordIdV10(Segment segment, Template template,
            PropertyTemplate propertyTemplate) {
        int ids = 1 + propertyTemplate.getIndex();
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        return segment.readRecordId(getOffset(0, ids));
    }

    private RecordId getRecordIdV11(Segment segment, Template template,
            PropertyTemplate propertyTemplate) {
        int ids = 1;
        if (template.getChildName() != Template.ZERO_CHILD_NODES) {
            ids++;
        }
        RecordId rid = segment.readRecordId(getOffset(0, ids));
        ListRecord pIds = new ListRecord(rid,
                template.getPropertyTemplates().length);
        return pIds.getEntry(propertyTemplate.getIndex());
    }

    @Override @Nonnull
    @Deprecated
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

        if (segment.getSegmentVersion().onOrAfter(V_11)) {
            if (propertyTemplates.length > 0) {
                ListRecord pIds = new ListRecord(
                        segment.readRecordId(getOffset(0, ids)),
                        propertyTemplates.length);
                for (int i = 0; i < propertyTemplates.length; i++) {
                    RecordId propertyId = pIds.getEntry(i);
                    list.add(new SegmentPropertyState(propertyId,
                            propertyTemplates[i]));
                }
            }
        } else {
            for (int i = 0; i < propertyTemplates.length; i++) {
                RecordId propertyId = segment.readRecordId(getOffset(0, ids++));
                list.add(new SegmentPropertyState(propertyId,
                        propertyTemplates[i]));
            }
        }

        return list;
    }

    @Override
    @Deprecated
    public boolean getBoolean(@Nonnull String name) {
        return Boolean.TRUE.toString().equals(getValueAsString(name, BOOLEAN));
    }

    @Override
    @Deprecated
    public long getLong(String name) {
        String value = getValueAsString(name, LONG);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return 0;
        }
    }

    @Override @CheckForNull
    @Deprecated
    public String getString(String name) {
        return getValueAsString(name, STRING);
    }

    @Override @Nonnull
    @Deprecated
    public Iterable<String> getStrings(@Nonnull String name) {
        return getValuesAsStrings(name, STRINGS);
    }

    @Override @CheckForNull
    @Deprecated
    public String getName(@Nonnull String name) {
        return getValueAsString(name, NAME);
    }

    @Override @Nonnull
    @Deprecated
    public Iterable<String> getNames(@Nonnull String name) {
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
        RecordId id;
        if (getSegment().getSegmentVersion().onOrAfter(V_11)) {
            id = getRecordIdV11(segment, template, propertyTemplate);
        } else {
            id = getRecordIdV10(segment, template, propertyTemplate);
        }
        return Segment.readString(id);
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
        RecordId id;
        if (getSegment().getSegmentVersion().onOrAfter(V_11)) {
            id = getRecordIdV11(segment, template, propertyTemplate);
        } else {
            id = getRecordIdV10(segment, template, propertyTemplate);
        }
        segment = id.getSegment();
        int size = segment.readInt(id.getOffset());
        if (size == 0) {
            return emptyList();
        }

        id = segment.readRecordId(id.getOffset() + 4);
        if (size == 1) {
            return singletonList(Segment.readString(id));
        }

        List<String> values = newArrayListWithCapacity(size);
        ListRecord list = new ListRecord(id, size);
        for (RecordId value : list.getEntries()) {
            values.add(Segment.readString(value));
        }
        return values;
    }

    @Override
    @Deprecated
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
    @Deprecated
    public boolean hasChildNode(@Nonnull String name) {
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
    @Deprecated
    public NodeState getChildNode(@Nonnull String name) {
        String childName = getTemplate().getChildName();
        if (childName == Template.MANY_CHILD_NODES) {
            MapEntry child = getChildNodeMap().getEntry(name);
            if (child != null) {
                return child.getNodeState();
            }
        } else if (childName != Template.ZERO_CHILD_NODES
                && childName.equals(name)) {
            Segment segment = getSegment();
            RecordId childNodeId = segment.readRecordId(getOffset(0, 1));
            return new SegmentNodeState(childNodeId);
        }
        checkValidName(name);
        return MISSING_NODE;
    }

    @Override @Nonnull
    @Deprecated
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
    @Deprecated
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
                    childName, new SegmentNodeState(childNodeId)));
        }
    }

    @Override @Nonnull
    @Deprecated
    public SegmentNodeBuilder builder() {
        return new SegmentNodeBuilder(this);
    }

    @Override
    @Deprecated
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base || fastEquals(this, base)) {
             return true; // no changes
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (!(base instanceof SegmentNodeState)) { // fallback
            return AbstractNodeState.compareAgainstBaseState(this, base, diff);
        }

        SegmentNodeState that = (SegmentNodeState) base;
        if (that.wasCompactedTo(this)) {
            return true; // no changes during compaction
        }

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
                    .compareTo(Integer.valueOf(beforeProperties[beforeIndex].hashCode()));
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

    @Override
    @Deprecated
    public boolean equals(Object object) {
        if (this == object || fastEquals(this, object)) {
            return true;
        } else if (object instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) object;
            Template template = getTemplate();
            return template.equals(that.getTemplate())
                    && template.compare(getRecordId(), that.getRecordId());
        } else {
            return object instanceof NodeState
                    && AbstractNodeState.equals(this, (NodeState) object); // TODO
        }
    }

    @Override
    @Deprecated
    public String toString() {
        return AbstractNodeState.toString(this);
    }

}
