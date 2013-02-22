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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.segment.MapRecord.Entry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

class Template {

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

        long count = state.getChildNodeCount();
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

    public String getChildName() {
        if (hasOneChildNode()) {
            return childName;
        } else {
            return null;
        }
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

    public PropertyState getProperty(
            String name, SegmentReader reader, RecordId recordId) {
        if ("jcr:primaryType".equals(name) && primaryType != null) {
            return primaryType;
        } else if ("jcr:mixinTypes".equals(name) && mixinTypes != null) {
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
                    return getProperty(reader, recordId, index);
                }
                index++;
            }
            return null;
        }
    }

    private PropertyState getProperty(
            SegmentReader reader, RecordId recordId, int index) {
        checkNotNull(reader);
        checkNotNull(recordId);
        checkElementIndex(index, properties.length);

        int offset = 1;
        if (!hasNoChildNodes()) {
            offset++;
        }
        return new SegmentPropertyState(
                properties[index], reader, reader.readRecordId(
                        recordId, (offset + index) * Segment.RECORD_ID_BYTES));
    }

    public Iterable<PropertyState> getProperties(
            SegmentReader reader, RecordId recordId) {
        List<PropertyState> list =
                Lists.newArrayListWithCapacity(properties.length + 2);
        if (primaryType != null) {
            list.add(primaryType);
        }
        if (mixinTypes != null) {
            list.add(mixinTypes);
        }
        int offset = 1;
        if (!hasNoChildNodes()) {
            offset++;
        }
        for (int i = 0; i < properties.length; i++) {
            RecordId propertyId = reader.readRecordId(
                    recordId, (offset + i) * Segment.RECORD_ID_BYTES);
            list.add(new SegmentPropertyState(
                    properties[i], reader, propertyId));
        }
        return list;
    }

    public long getChildNodeCount(SegmentReader reader, RecordId recordId) {
        if (hasNoChildNodes()) {
            return 0;
        } else if (hasManyChildNodes()) {
            RecordId childNodesId =
                    reader.readRecordId(recordId, Segment.RECORD_ID_BYTES);
            return MapRecord.readMap(reader.getStore(), childNodesId).size();
        } else {
            return 1;
        }
    }

    MapRecord getChildNodeMap(SegmentReader reader, RecordId recordId) {
        checkState(hasManyChildNodes());
        return MapRecord.readMap(
                reader.getStore(),
                reader.readRecordId(recordId, Segment.RECORD_ID_BYTES));
    }

    public boolean hasChildNode(
            String name, SegmentReader reader, RecordId recordId) {
        if (hasNoChildNodes()) {
            return false;
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(reader, recordId);
            return map.getEntry(name) != null;
        } else {
            return name.equals(childName);
        }
    }

    public NodeState getChildNode(
            String name, SegmentReader reader, RecordId recordId) {
        if (hasNoChildNodes()) {
            return null;
        } else if (hasManyChildNodes()) {
            RecordId childNodeId =
                    getChildNodeMap(reader, recordId).getEntry(name);
            if (childNodeId != null) {
                return new SegmentNodeState(reader, childNodeId);
            } else {
                return null;
            }
        } else if (name.equals(childName)) {
            RecordId childNodeId =
                    reader.readRecordId(recordId, Segment.RECORD_ID_BYTES);
            return new SegmentNodeState(reader, childNodeId);
        } else {
            return null;
        }
    }

    public Iterable<String> getChildNodeNames(
            SegmentReader reader, RecordId recordId) {
        if (hasNoChildNodes()) {
            return Collections.emptyList();
        } else if (hasManyChildNodes()) {
            return getChildNodeMap(reader, recordId).getKeys();
        } else {
            return Collections.singletonList(childName);
        }
    }

    public Iterable<? extends ChildNodeEntry> getChildNodeEntries(
            final SegmentReader reader, RecordId recordId) {
        if (hasNoChildNodes()) {
            return Collections.emptyList();
        } else if (hasManyChildNodes()) {
            return Iterables.transform(
                    getChildNodeMap(reader, recordId).getEntries(),
                    new Function<MapRecord.Entry, ChildNodeEntry>() {
                        @Override @Nullable
                        public ChildNodeEntry apply(@Nullable Entry input) {
                            return new MemoryChildNodeEntry(
                                    input.getKey(),
                                    new SegmentNodeState(reader, input.getValue()));
                        }
                    });
        } else {
            RecordId childNodeId =
                    reader.readRecordId(recordId, Segment.RECORD_ID_BYTES);
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, new SegmentNodeState(reader, childNodeId)));
        }
    }

    public void compareAgainstBaseState(
            SegmentReader reader, RecordId afterId,
            Template beforeTemplate, RecordId beforeId,
            NodeStateDiff diff) {
        checkNotNull(reader);
        checkNotNull(afterId);
        checkNotNull(beforeTemplate);
        checkNotNull(beforeId);
        checkNotNull(diff);

        // Compare type properties
        compareProperties(beforeTemplate.primaryType, primaryType, diff);
        compareProperties(beforeTemplate.mixinTypes, mixinTypes, diff);

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
                afterProperty = getProperty(reader, afterId, afterIndex++);
            } else if (d > 0) {
                beforeProperty = beforeTemplate.getProperty(
                        reader, beforeId, beforeIndex++);
            } else {
                afterProperty = getProperty(reader, afterId, afterIndex++);
                beforeProperty = beforeTemplate.getProperty(
                        reader, beforeId, beforeIndex++);
            }
            compareProperties(beforeProperty, afterProperty, diff);
        }
        while (afterIndex < properties.length) {
            diff.propertyAdded(getProperty(reader, afterId, afterIndex++));
        }
        while (beforeIndex < beforeTemplate.properties.length) {
            diff.propertyDeleted(beforeTemplate.getProperty(
                    reader, beforeId, beforeIndex++));
        }

        if (hasNoChildNodes()) {
            if (!beforeTemplate.hasNoChildNodes()) {
                for (ChildNodeEntry entry :
                        beforeTemplate.getChildNodeEntries(reader, beforeId)) {
                    diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState());
                }
            }
        } else if (hasOneChildNode()) {
            NodeState afterNode = getChildNode(childName, reader, afterId);
            NodeState beforeNode = beforeTemplate.getChildNode(
                    childName, reader, beforeId);
            if (beforeNode == null) {
                diff.childNodeAdded(childName, afterNode);
            } else if (!beforeNode.equals(afterNode)) {
                diff.childNodeChanged(childName, beforeNode, afterNode);
            }
            if ((beforeTemplate.hasOneChildNode() && beforeNode == null)
                    || beforeTemplate.hasManyChildNodes()) {
                for (ChildNodeEntry entry :
                    beforeTemplate.getChildNodeEntries(reader, beforeId)) {
                    if (!childName.equals(entry.getName())) {
                        diff.childNodeDeleted(
                                entry.getName(), entry.getNodeState());
                    }
                }
            }
        } else {
            // TODO: Leverage the HAMT data structure for the comparison
            Set<String> baseChildNodes = new HashSet<String>();
            for (ChildNodeEntry beforeCNE
                    : beforeTemplate.getChildNodeEntries(reader, beforeId)) {
                String name = beforeCNE.getName();
                NodeState beforeChild = beforeCNE.getNodeState();
                NodeState afterChild = getChildNode(name, reader, afterId);
                if (afterChild == null) {
                    diff.childNodeDeleted(name, beforeChild);
                } else {
                    baseChildNodes.add(name);
                    if (!beforeChild.equals(afterChild)) {
                        diff.childNodeChanged(name, beforeChild, afterChild);
                    }
                }
            }
            for (ChildNodeEntry afterChild
                    : getChildNodeEntries(reader, afterId)) {
                String name = afterChild.getName();
                if (!baseChildNodes.contains(name)) {
                    diff.childNodeAdded(name, afterChild.getNodeState());
                }
            }
        }
    }

    private void compareProperties(
            PropertyState before, PropertyState after, NodeStateDiff diff) {
        if (before == null) {
            if (after != null) {
                diff.propertyAdded(after);
            }
        } else if (after == null) {
            diff.propertyDeleted(before);
        } else if (!before.equals(after)) {
            diff.propertyChanged(before, after);
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
