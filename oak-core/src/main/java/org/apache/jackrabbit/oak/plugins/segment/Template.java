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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

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
            String name, SegmentStore store, RecordId recordId) {
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
                    return getProperty(store, recordId, index);
                }
                index++;
            }
            return null;
        }
    }

    private PropertyState getProperty(
            SegmentStore store, RecordId recordId, int index) {
        checkNotNull(store);
        checkNotNull(recordId);
        checkElementIndex(index, properties.length);

        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        if (!hasNoChildNodes()) {
            offset += RECORD_ID_BYTES;
        }
        offset += index * RECORD_ID_BYTES;
        Segment segment = store.readSegment(recordId.getSegmentId());
        return new SegmentPropertyState(
                properties[index], store, segment.readRecordId(offset));
    }

    public Iterable<PropertyState> getProperties(
            SegmentStore store, RecordId recordId) {
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
        Segment segment = store.readSegment(recordId.getSegmentId());
        for (int i = 0; i < properties.length; i++) {
            RecordId propertyId = segment.readRecordId(offset);
            list.add(new SegmentPropertyState(
                    properties[i], store, propertyId));
            offset += RECORD_ID_BYTES;
        }
        return list;
    }

    public long getChildNodeCount(SegmentStore store, RecordId recordId) {
        if (hasNoChildNodes()) {
            return 0;
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(store, recordId);
            return map.size();
        } else {
            return 1;
        }
    }

    MapRecord getChildNodeMap(SegmentStore store, RecordId recordId) {
        checkState(hasManyChildNodes());
        int offset = recordId.getOffset() + RECORD_ID_BYTES;
        Segment segment = store.readSegment(recordId.getSegmentId());
        RecordId childNodesId = segment.readRecordId(offset);
        return MapRecord.readMap(store, childNodesId);
    }

    public boolean hasChildNode(
            String name, SegmentStore store, RecordId recordId) {
        if (hasNoChildNodes()) {
            return false;
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(store, recordId);
            return map.getEntry(name) != null;
        } else {
            return name.equals(childName);
        }
    }

    public NodeState getChildNode(
            String name, SegmentStore store, RecordId recordId) {
        if (hasNoChildNodes()) {
            return MISSING_NODE;
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(store, recordId);
            RecordId childNodeId = map.getEntry(name);
            if (childNodeId != null) {
                return new SegmentNodeState(store, childNodeId);
            } else {
                return MISSING_NODE;
            }
        } else if (name.equals(childName)) {
            int offset = recordId.getOffset() + RECORD_ID_BYTES;
            Segment segment = store.readSegment(recordId.getSegmentId());
            RecordId childNodeId = segment.readRecordId(offset);
            return new SegmentNodeState(store, childNodeId);
        } else {
            return MISSING_NODE;
        }
    }

    Iterable<String> getChildNodeNames(SegmentStore store, RecordId recordId) {
        if (hasNoChildNodes()) {
            return Collections.emptyList();
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(store, recordId);
            return map.getKeys();
        } else {
            return Collections.singletonList(childName);
        }
    }

    Iterable<? extends ChildNodeEntry> getChildNodeEntries(
            SegmentStore store, RecordId recordId) {
        if (hasNoChildNodes()) {
            return Collections.emptyList();
        } else if (hasManyChildNodes()) {
            MapRecord map = getChildNodeMap(store, recordId);
            return map.getEntries();
        } else {
            int offset = recordId.getOffset() + RECORD_ID_BYTES;
            Segment segment = store.readSegment(recordId.getSegmentId());
            RecordId childNodeId = segment.readRecordId(offset);
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, new SegmentNodeState(store, childNodeId)));
        }
    }

    public boolean compareAgainstBaseState(
            SegmentStore store, RecordId afterId,
            Template beforeTemplate, RecordId beforeId,
            NodeStateDiff diff) {
        checkNotNull(store);
        checkNotNull(afterId);
        checkNotNull(beforeTemplate);
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
                afterProperty = getProperty(store, afterId, afterIndex++);
            } else if (d > 0) {
                beforeProperty = beforeTemplate.getProperty(
                        store, beforeId, beforeIndex++);
            } else {
                afterProperty = getProperty(store, afterId, afterIndex++);
                beforeProperty = beforeTemplate.getProperty(
                        store, beforeId, beforeIndex++);
            }
            if (!compareProperties(beforeProperty, afterProperty, diff)) {
                return false;
            }
        }
        while (afterIndex < properties.length) {
            if (!diff.propertyAdded(getProperty(store, afterId, afterIndex++))) {
                return false;
            }
        }
        while (beforeIndex < beforeTemplate.properties.length) {
            PropertyState beforeProperty = beforeTemplate.getProperty(
                    store, beforeId, beforeIndex++);
            if (!diff.propertyDeleted(beforeProperty)) {
                return false;
            }
        }

        if (hasNoChildNodes()) {
            if (!beforeTemplate.hasNoChildNodes()) {
                for (ChildNodeEntry entry :
                        beforeTemplate.getChildNodeEntries(store, beforeId)) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            }
        } else if (hasOneChildNode()) {
            NodeState afterNode = getChildNode(childName, store, afterId);
            NodeState beforeNode = beforeTemplate.getChildNode(
                    childName, store, beforeId);
            if (!beforeNode.exists()) {
                if (!diff.childNodeAdded(childName, afterNode)) {
                    return false;
                }
            } else if (!beforeNode.equals(afterNode)) {
                if (!diff.childNodeChanged(childName, beforeNode, afterNode)) {
                    return false;
                }
            }
            if ((beforeTemplate.hasOneChildNode() && !beforeNode.exists())
                    || beforeTemplate.hasManyChildNodes()) {
                for (ChildNodeEntry entry :
                    beforeTemplate.getChildNodeEntries(store, beforeId)) {
                    if (!childName.equals(entry.getName())) {
                        if (!diff.childNodeDeleted(
                                entry.getName(), entry.getNodeState())) {
                            return false;
                        }
                    }
                }
            }
        } else {
            // TODO: Leverage the HAMT data structure for the comparison
            Set<String> baseChildNodes = new HashSet<String>();
            for (ChildNodeEntry beforeCNE
                    : beforeTemplate.getChildNodeEntries(store, beforeId)) {
                String name = beforeCNE.getName();
                NodeState beforeChild = beforeCNE.getNodeState();
                NodeState afterChild = getChildNode(name, store, afterId);
                if (!afterChild.exists()) {
                    if (!diff.childNodeDeleted(name, beforeChild)) {
                        return false;
                    }
                } else {
                    baseChildNodes.add(name);
                    if (!beforeChild.equals(afterChild)) {
                        if (!diff.childNodeChanged(name, beforeChild, afterChild)) {
                            return false;
                        }
                    }
                }
            }
            for (ChildNodeEntry afterChild
                    : getChildNodeEntries(store, afterId)) {
                String name = afterChild.getName();
                if (!baseChildNodes.contains(name)) {
                    if (!diff.childNodeAdded(name, afterChild.getNodeState())) {
                        return false;
                    }
                }
            }
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
