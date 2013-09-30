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

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.segment.MapRecord.MapDiff;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class Template {

    static final String ZERO_CHILD_NODES = null;

    static final String MANY_CHILD_NODES = "";

    /**
     * The {@code jcr:primaryType} property, if present as a single-valued
     * {@code NAME} property. Otherwise {@code null}.
     */
    @CheckForNull
    private final PropertyState primaryType;

    /**
     * The {@code jcr:mixinTypes} property, if present as a multi-valued
     * {@code NAME} property. Otherwise {@code null}.
     */
    @CheckForNull
    private final PropertyState mixinTypes;

    /**
     * Templates of all the properties of a node, excluding the
     * above-mentioned {@code NAME}-valued type properties, if any.
     */
    @Nonnull
    private final PropertyTemplate[] properties;

    /**
     * Name of the single child node, if the node contains just one child.
     * Otherwise {@link #ZERO_CHILD_NODES} (i.e. {@code null}) if there are
     * no children, or {@link #MANY_CHILD_NODES} if there are more than one.
     */
    @CheckForNull
    private final String childName;

    Template(
            PropertyState primaryType, PropertyState mixinTypes,
            PropertyTemplate[] properties, String childName) {
        this.primaryType = primaryType;
        this.mixinTypes = mixinTypes;
        this.properties = properties;
        this.childName = childName;
    }

    Template(NodeState state) {
        PropertyState primary = null;
        PropertyState mixins = null;
        List<PropertyTemplate> templates = Lists.newArrayList();

        for (PropertyState property : state.getProperties()) {
            String name = property.getName();
            Type<?> type = property.getType();
            if ("jcr:primaryType".equals(name) && type == Type.NAME) {
                primary = property;
            } else if ("jcr:mixinTypes".equals(name) && type == Type.NAMES) {
                mixins = property;
            } else {
                templates.add(new PropertyTemplate(property));
            }
        }

        this.primaryType = primary;
        this.mixinTypes = mixins;
        this.properties =
                templates.toArray(new PropertyTemplate[templates.size()]);
        Arrays.sort(properties);

        long count = state.getChildNodeCount(2);
        if (count == 0) {
            childName = ZERO_CHILD_NODES;
        } else if (count == 1) {
            childName = state.getChildNodeNames().iterator().next();
            checkState(childName != null && !childName.equals(MANY_CHILD_NODES));
        } else {
            childName = MANY_CHILD_NODES;
        }
    }

    public boolean hasPrimaryType() {
        return primaryType != null;
    }

    public String getPrimaryType() {
        if (primaryType != null) {
            return primaryType.getValue(Type.NAME);
        } else {
            return null;
        }
    }

    public boolean hasMixinTypes() {
        return mixinTypes != null;
    }

    public Iterable<String> getMixinTypes() {
        if (mixinTypes != null) {
            return mixinTypes.getValue(Type.NAMES);
        } else {
            return null;
        }
    }

    public PropertyTemplate[] getPropertyTemplates() {
        return properties;
    }

    public boolean hasNoChildNodes() {
        return childName == ZERO_CHILD_NODES;
    }

    public boolean hasOneChildNode() {
        return !hasNoChildNodes() && !hasManyChildNodes();
    }

    public boolean hasManyChildNodes() {
        return childName == MANY_CHILD_NODES;
    }

    String getChildName() {
        return childName;
    }

    public int getPropertyCount() {
        if (primaryType != null && mixinTypes != null) {
            return properties.length + 2;
        } else if (primaryType != null || mixinTypes != null) {
            return properties.length + 1;
        } else {
            return properties.length;
        }
    }

    public boolean hasProperty(String name) {
        if (JCR_PRIMARYTYPE.equals(name)) {
            return primaryType != null;
        } else if (JCR_MIXINTYPES.equals(name)) {
            return mixinTypes != null;
        } else {
            int hash = name.hashCode();
            int index = 0;
            while (index < properties.length
                    && properties[index].getName().hashCode() < hash) {
                index++;
            }
            while (index < properties.length
                    && properties[index].getName().hashCode() == hash) {
                if (name.equals(properties[index].getName())) {
                    return true;
                }
                index++;
            }
            return false;
        }
    }

    public PropertyState getProperty(
            String name, Segment segment, RecordId recordId) {
        if (JCR_PRIMARYTYPE.equals(name) && primaryType != null) {
            return primaryType;
        } else if (JCR_MIXINTYPES.equals(name) && mixinTypes != null) {
            return mixinTypes;
        } else {
            int hash = name.hashCode();
            int index = 0;
            while (index < properties.length
                    && properties[index].getName().hashCode() < hash) {
                index++;
            }
            while (index < properties.length
                    && properties[index].getName().hashCode() == hash) {
                if (name.equals(properties[index].getName())) {
                    return getProperty(segment, recordId, index);
                }
                index++;
            }
            return null;
        }
    }

    private PropertyState getProperty(
            Segment segment, RecordId recordId, int index) {
        checkElementIndex(index, properties.length);
        segment = checkNotNull(segment).getSegment(checkNotNull(recordId));

        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        if (!hasNoChildNodes()) {
            offset += RECORD_ID_BYTES;
        }
        offset += index * RECORD_ID_BYTES;
        return new SegmentPropertyState(
                segment, segment.readRecordId(offset), properties[index]);
    }

    public Iterable<PropertyState> getProperties(
            Segment segment, RecordId recordId) {
        List<PropertyState> list =
                Lists.newArrayListWithCapacity(properties.length + 2);
        if (primaryType != null) {
            list.add(primaryType);
        }
        if (mixinTypes != null) {
            list.add(mixinTypes);
        }
        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        if (!hasNoChildNodes()) {
            offset += RECORD_ID_BYTES;
        }
        segment = segment.getSegment(recordId);
        for (int i = 0; i < properties.length; i++) {
            RecordId propertyId = segment.readRecordId(offset);
            list.add(new SegmentPropertyState(
                    segment, propertyId, properties[i]));
            offset += RECORD_ID_BYTES;
        }
        return list;
    }

    MapRecord getChildNodeMap(Segment segment, RecordId recordId) {
        checkState(hasManyChildNodes());
        segment = segment.getSegment(recordId);
        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        RecordId childNodesId = segment.readRecordId(offset);
        return segment.readMap(childNodesId);
    }

    public NodeState getChildNode(
            String name, Segment segment, RecordId recordId) {
        if (hasNoChildNodes()) {
            return MISSING_NODE;
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(segment, recordId);
            RecordId childNodeId = map.getEntry(name);
            if (childNodeId != null) {
                return new SegmentNodeState(segment, childNodeId);
            } else {
                return MISSING_NODE;
            }
        } else if (name.equals(childName)) {
            segment = segment.getSegment(recordId);
            int offset = recordId.getOffset() + RECORD_ID_BYTES;
            RecordId childNodeId = segment.readRecordId(offset);
            return new SegmentNodeState(segment, childNodeId);
        } else {
            return MISSING_NODE;
        }
    }

    Iterable<? extends ChildNodeEntry> getChildNodeEntries(
            Segment segment, RecordId recordId) {
        if (hasNoChildNodes()) {
            return Collections.emptyList();
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(segment, recordId);
            return map.getEntries();
        } else {
            segment = segment.getSegment(recordId);
            int offset = recordId.getOffset() + RECORD_ID_BYTES;
            RecordId childNodeId = segment.readRecordId(offset);
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, new SegmentNodeState(segment, childNodeId)));
        }
    }

    public boolean compare(
            Segment thisSegment, RecordId thisId, Segment thatSegment, RecordId thatId) {
        checkNotNull(thisId);
        checkNotNull(thatId);

        // Compare properties
        for (int i = 0; i < properties.length; i++) {
            PropertyState thisProperty = getProperty(thisSegment, thisId, i);
            PropertyState thatProperty = getProperty(thatSegment, thatId, i);
            if (!thisProperty.equals(thatProperty)) {
                return false;
            }
        }

        // Compare child nodes
        if (hasNoChildNodes()) {
            return true;
        } else if (hasOneChildNode()) {
            NodeState thisChild = getChildNode(childName, thisSegment, thisId);
            NodeState thatChild = getChildNode(childName, thatSegment, thatId);
            return thisChild.equals(thatChild);
        } else {
            // TODO: Leverage the HAMT data structure for the comparison
            MapRecord thisMap = getChildNodeMap(thisSegment, thisId);
            MapRecord thatMap = getChildNodeMap(thatSegment, thatId);
            if (thisMap.getRecordId().equals(thatMap.getRecordId())) {
                return true; // shortcut
            } else if (thisMap.size() != thatMap.size()) {
                return false; // shortcut
            } else {
                // TODO: can this be optimized?
                for (MapEntry entry : thisMap.getEntries()) {
                    String name = entry.getName();
                    RecordId thisChild = entry.getValue();
                    RecordId thatChild = thatMap.getEntry(name);
                    if (thatChild == null) {
                        return false;
                    } else if (!thisChild.equals(thatChild)
                            && !new SegmentNodeState(thisSegment, thisChild).equals(
                                    new SegmentNodeState(thatSegment, thatChild))) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    public boolean compareAgainstBaseState(
            final Segment segment, RecordId afterId,
            Template beforeTemplate, RecordId beforeId,
            final NodeStateDiff diff) {
        checkNotNull(segment);
        checkNotNull(afterId);
        checkNotNull(beforeTemplate);
        checkNotNull(beforeId);
        checkNotNull(diff);

        // Compare type properties
        if (!compareProperties(beforeTemplate.primaryType, primaryType, diff)
                || !compareProperties(beforeTemplate.mixinTypes, mixinTypes, diff)) {
            return false;
        }

        final Segment afterSegment = segment.getSegment(afterId);
        final Segment beforeSegment = segment.getSegment(beforeId);

        // Compare other properties, leveraging the ordering
        int beforeIndex = 0;
        int afterIndex = 0;
        while (beforeIndex < beforeTemplate.properties.length
                && afterIndex < properties.length) {
            int d = Integer.valueOf(properties[afterIndex].hashCode())
                    .compareTo(Integer.valueOf(beforeTemplate.properties[beforeIndex].hashCode()));
            if (d == 0) {
                d = properties[afterIndex].getName().compareTo(
                        beforeTemplate.properties[beforeIndex].getName());
            }
            PropertyState beforeProperty = null;
            PropertyState afterProperty = null;
            if (d < 0) {
                afterProperty = getProperty(afterSegment, afterId, afterIndex++);
            } else if (d > 0) {
                beforeProperty = beforeTemplate.getProperty(
                        beforeSegment, beforeId, beforeIndex++);
            } else {
                afterProperty = getProperty(afterSegment, afterId, afterIndex++);
                beforeProperty = beforeTemplate.getProperty(
                        beforeSegment, beforeId, beforeIndex++);
            }
            if (!compareProperties(beforeProperty, afterProperty, diff)) {
                return false;
            }
        }
        while (afterIndex < properties.length) {
            if (!diff.propertyAdded(getProperty(afterSegment, afterId, afterIndex++))) {
                return false;
            }
        }
        while (beforeIndex < beforeTemplate.properties.length) {
            PropertyState beforeProperty = beforeTemplate.getProperty(
                    beforeSegment, beforeId, beforeIndex++);
            if (!diff.propertyDeleted(beforeProperty)) {
                return false;
            }
        }

        if (hasNoChildNodes()) {
            if (!beforeTemplate.hasNoChildNodes()) {
                for (ChildNodeEntry entry :
                        beforeTemplate.getChildNodeEntries(beforeSegment, beforeId)) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            }
        } else if (hasOneChildNode()) {
            NodeState afterNode = getChildNode(childName, afterSegment, afterId);
            NodeState beforeNode = beforeTemplate.getChildNode(
                    childName, beforeSegment, beforeId);
            if (!beforeNode.exists()) {
                if (!diff.childNodeAdded(childName, afterNode)) {
                    return false;
                }
            } else if (!fastEquals(afterNode, beforeNode)) {
                if (!diff.childNodeChanged(childName, beforeNode, afterNode)) {
                    return false;
                }
            }
            if ((beforeTemplate.hasOneChildNode() && !beforeNode.exists())
                    || beforeTemplate.hasManyChildNodes()) {
                for (ChildNodeEntry entry :
                    beforeTemplate.getChildNodeEntries(beforeSegment, beforeId)) {
                    if (!childName.equals(entry.getName())) {
                        if (!diff.childNodeDeleted(
                                entry.getName(), entry.getNodeState())) {
                            return false;
                        }
                    }
                }
            }
        } else if (beforeTemplate.hasNoChildNodes()) {
            for (ChildNodeEntry entry : getChildNodeEntries(afterSegment, afterId)) {
                if (!diff.childNodeAdded(
                        entry.getName(), entry.getNodeState())) {
                    return false;
                }
            }
        } else if (beforeTemplate.hasOneChildNode()) {
            String name = beforeTemplate.getChildName();
            for (ChildNodeEntry entry : getChildNodeEntries(afterSegment, afterId)) {
                String childName = entry.getName();
                NodeState afterChild = entry.getNodeState();
                if (name.equals(childName)) {
                    NodeState beforeChild =
                            beforeTemplate.getChildNode(name, beforeSegment, beforeId);
                    if (beforeChild.exists()) {
                        if (!fastEquals(afterChild, beforeChild)
                                && !diff.childNodeChanged(
                                        childName, beforeChild, afterChild)) {
                            return false;
                        }
                    } else {
                        if (!diff.childNodeAdded(childName, afterChild)) {
                            return false;
                        }
                    }
                } else if (!diff.childNodeAdded(childName, afterChild)) {
                    return false;
                }
            }
        } else {
            // TODO: Leverage the HAMT data structure for the comparison
            MapRecord afterMap = getChildNodeMap(afterSegment, afterId);
            MapRecord beforeMap = beforeTemplate.getChildNodeMap(beforeSegment, beforeId);
            return afterMap.compare(beforeMap, new MapDiff() {
                @Override
                public boolean entryAdded(String key, RecordId after) {
                    return diff.childNodeAdded(
                            key, new SegmentNodeState(afterSegment, after));
                }
                @Override
                public boolean entryChanged(
                        String key, RecordId before, RecordId after) {
                    SegmentNodeState b = new SegmentNodeState(beforeSegment, before);
                    SegmentNodeState a = new SegmentNodeState(afterSegment, after);
                    return fastEquals(a, b) || diff.childNodeChanged(key, b, a);
                }
                @Override
                public boolean entryDeleted(String key, RecordId before) {
                    return diff.childNodeDeleted(
                            key, new SegmentNodeState(beforeSegment, before));
                }
            });
        }

        return true;
    }

    public boolean compareAgainstEmptyState(
            Segment segment, RecordId recordId, final NodeStateDiff diff) {
        checkNotNull(segment);
        checkNotNull(recordId);
        checkNotNull(diff);

        // Type properties
        if (primaryType != null && !diff.propertyAdded(primaryType)) {
            return false;
        }
        if (mixinTypes != null && !diff.propertyAdded(mixinTypes)) {
            return false;
        }

        segment = segment.getSegment(recordId);
        int offset = recordId.getOffset() + RECORD_ID_BYTES;

        if (childName == MANY_CHILD_NODES) {
            RecordId childNodesId = segment.readRecordId(offset);
            MapRecord children = segment.readMap(childNodesId);
            final Segment s = segment;
            children.compareAgainstEmptyMap(new MapDiff() {
                @Override
                public boolean entryAdded(String key, RecordId after) {
                    return diff.childNodeAdded(
                            key, new SegmentNodeState(s, after));
                }
                @Override
                public boolean entryChanged(
                        String key, RecordId before, RecordId after) {
                    throw new IllegalStateException();
                }
                @Override
                public boolean entryDeleted(String key, RecordId before) {
                    throw new IllegalStateException();
                }
            });
            offset += RECORD_ID_BYTES;
        } else if (childName != ZERO_CHILD_NODES) {
            RecordId childNodeId = segment.readRecordId(offset);
            if (!diff.childNodeAdded(
                    childName, new SegmentNodeState(segment, childNodeId))) {
                return false;
            }
            offset += RECORD_ID_BYTES;
        }

        // Other properties
        for (int i = 0; i < properties.length; i++) {
            if (!diff.propertyAdded(new SegmentPropertyState(
                    segment, segment.readRecordId(offset), properties[i]))) {
                return false;
            }
            offset += RECORD_ID_BYTES;
        }

        return true;
    }


    private boolean fastEquals(NodeState a, NodeState b) {
        if (a == b) {
            return true;
        } else if (a instanceof SegmentNodeState
                && b instanceof SegmentNodeState) {
            return ((SegmentNodeState) a).getRecordId().equals(
                    ((SegmentNodeState) b).getRecordId());
        } else {
            return false;
        }
    }

    private boolean compareProperties(
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
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof Template) {
            Template that = (Template) object;
            return Objects.equal(primaryType, that.primaryType)
                    && Objects.equal(mixinTypes, that.mixinTypes)
                    && Arrays.equals(properties, that.properties)
                    && Objects.equal(childName, that.childName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                primaryType, mixinTypes, Arrays.asList(properties), childName);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        if (primaryType != null) {
            builder.append(primaryType);
            builder.append(", ");
        }
        if (mixinTypes != null) {
            builder.append(mixinTypes);
            builder.append(", ");
        }
        for (int i = 0; i < properties.length; i++) {
            builder.append(properties[i]);
            builder.append(" = ?, ");
        }
        if (hasNoChildNodes()) {
            builder.append("<no children>");
        } else if (hasManyChildNodes()) {
            builder.append("<many children>");
        } else {
            builder.append(childName + " = <node>");
        }
        builder.append(" }");
        return builder.toString();
    }

}
