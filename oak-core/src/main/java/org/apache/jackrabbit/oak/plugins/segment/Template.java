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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.Record.fastEquals;
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

    PropertyState getPrimaryType() {
        return primaryType;
    }

    PropertyState getMixinTypes() {
        return mixinTypes;
    }

    PropertyTemplate[] getPropertyTemplates() {
        return properties;
    }

    /**
     * Returns the template of the named property, or {@code null} if no such
     * property exists. Use the {@link #getPrimaryType()} and
     * {@link #getMixinTypes()} for accessing the JCR type properties, as
     * they don't have templates.
     *
     * @param name property name
     * @return property template, or {@code} null if not found
     */
    PropertyTemplate getPropertyTemplate(String name) {
        int hash = name.hashCode();
        int index = 0;
        while (index < properties.length
                && properties[index].getName().hashCode() < hash) {
            index++;
        }
        while (index < properties.length
                && properties[index].getName().hashCode() == hash) {
            if (name.equals(properties[index].getName())) {
                return properties[index];
            }
            index++;
        }
        return null;
    }

    String getChildName() {
        return childName;
    }

    private PropertyState getProperty(
            Segment segment, RecordId recordId, int index) {
        checkElementIndex(index, properties.length);
        segment = checkNotNull(segment).getSegment(checkNotNull(recordId));

        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        if (childName != ZERO_CHILD_NODES) {
            offset += RECORD_ID_BYTES;
        }
        offset += index * RECORD_ID_BYTES;
        return new SegmentPropertyState(
                segment, segment.readRecordId(offset), properties[index]);
    }

    MapRecord getChildNodeMap(Segment segment, RecordId recordId) {
        checkState(childName != ZERO_CHILD_NODES);
        segment = segment.getSegment(recordId);
        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        RecordId childNodesId = segment.readRecordId(offset);
        return segment.readMap(childNodesId);
    }

    public NodeState getChildNode(
            String name, Segment segment, RecordId recordId) {
        if (childName == ZERO_CHILD_NODES) {
            return MISSING_NODE;
        } else if (childName == MANY_CHILD_NODES) {
            MapRecord map = getChildNodeMap(segment, recordId);
            MapEntry child = map.getEntry(name);
            if (child != null) {
                return child.getNodeState();
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
        if (childName == ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == MANY_CHILD_NODES) {
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
        if (childName == ZERO_CHILD_NODES) {
            return true;
        } else if (childName != MANY_CHILD_NODES) {
            NodeState thisChild = getChildNode(childName, thisSegment, thisId);
            NodeState thatChild = getChildNode(childName, thatSegment, thatId);
            return thisChild.equals(thatChild);
        } else {
            // TODO: Leverage the HAMT data structure for the comparison
            MapRecord thisMap = getChildNodeMap(thisSegment, thisId);
            MapRecord thatMap = getChildNodeMap(thatSegment, thatId);
            if (fastEquals(thisMap, thatMap)) {
                return true; // shortcut
            } else if (thisMap.size() != thatMap.size()) {
                return false; // shortcut
            } else {
                // TODO: can this be optimized?
                for (MapEntry entry : thisMap.getEntries()) {
                    String name = entry.getName();
                    MapEntry thatEntry = thatMap.getEntry(name);
                    if (thatEntry == null) {
                        return false;
                    } else if (!entry.getNodeState().equals(thatEntry.getNodeState())) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    public boolean compareAgainstBaseState(
            Segment afterSegment, RecordId afterId,
            Template beforeTemplate, Segment beforeSegment, RecordId beforeId,
            final NodeStateDiff diff) {
        checkNotNull(afterSegment);
        checkNotNull(afterId);
        checkNotNull(beforeTemplate);
        checkNotNull(beforeSegment);
        checkNotNull(beforeId);
        checkNotNull(diff);

        // Compare type properties
        if (!compareProperties(beforeTemplate.primaryType, primaryType, diff)
                || !compareProperties(beforeTemplate.mixinTypes, mixinTypes, diff)) {
            return false;
        }

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

        if (childName == ZERO_CHILD_NODES) {
            if (beforeTemplate.childName != ZERO_CHILD_NODES) {
                for (ChildNodeEntry entry : beforeTemplate.getChildNodeEntries(
                        beforeSegment, beforeId)) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            }
        } else if (childName != MANY_CHILD_NODES) {
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
            if (beforeTemplate.childName == MANY_CHILD_NODES
                    || (beforeTemplate.childName != ZERO_CHILD_NODES
                        && !beforeNode.exists())) {
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
        } else if (beforeTemplate.childName == ZERO_CHILD_NODES) {
            for (ChildNodeEntry entry : getChildNodeEntries(afterSegment, afterId)) {
                if (!diff.childNodeAdded(
                        entry.getName(), entry.getNodeState())) {
                    return false;
                }
            }
        } else if (beforeTemplate.childName != MANY_CHILD_NODES) {
            String name = beforeTemplate.childName;
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
            return afterMap.compare(beforeMap, diff);
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
            for (MapEntry entry : children.getEntries()) {
                if (!diff.childNodeAdded(
                        entry.getName(), entry.getNodeState())) {
                    return false;
                }
            }
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
        if (childName == ZERO_CHILD_NODES) {
            builder.append("<no children>");
        } else if (childName == MANY_CHILD_NODES) {
            builder.append("<many children>");
        } else {
            builder.append(childName + " = <node>");
        }
        builder.append(" }");
        return builder.toString();
    }

}
