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

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.segment.Segment.RECORD_ID_BYTES;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The in-memory representation of a "hidden class" of a node; inspired by the
 * Chrome V8 Javascript engine).
 * <p>
 * Templates are always read fully in-memory.
 */
public class Template {

    static final short ZERO_CHILD_NODES_TYPE = 0;

    static final short SINGLE_CHILD_NODE_TYPE = 1;

    static final short MANY_CHILD_NODES_TYPE = 2;

    static final String ZERO_CHILD_NODES = null;

    static final String MANY_CHILD_NODES = "";

    @Nonnull
    private final SegmentReader reader;

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

    Template(@Nonnull SegmentReader reader,
             @Nullable PropertyState primaryType,
             @Nullable PropertyState mixinTypes,
             @Nullable  PropertyTemplate[] properties,
             @Nullable String childName) {
        this.reader = checkNotNull(reader);
        this.primaryType = primaryType;
        this.mixinTypes = mixinTypes;
        if (properties != null) {
            this.properties = properties;
            Arrays.sort(this.properties);
        } else {
            this.properties = new PropertyTemplate[0];
        }
        this.childName = childName;
    }

    Template(@Nonnull SegmentReader reader, @Nonnull NodeState state) {
        this.reader = checkNotNull(reader);
        checkNotNull(state);
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

    @CheckForNull
    PropertyState getPrimaryType() {
        return primaryType;
    }

    @CheckForNull
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

    @CheckForNull
    String getChildName() {
        return childName;
    }

    SegmentPropertyState getProperty(RecordId recordId, int index) {
        checkElementIndex(index, properties.length);
        Segment segment = checkNotNull(recordId).getSegment();

        int offset = 2 * RECORD_ID_BYTES;
        if (childName != ZERO_CHILD_NODES) {
            offset += RECORD_ID_BYTES;
        }
        RecordId lid = segment.readRecordId(recordId.getRecordNumber(), offset);
        ListRecord props = new ListRecord(lid, properties.length);
        RecordId rid = props.getEntry(index);
        return reader.readProperty(rid, properties[index]);
    }

    MapRecord getChildNodeMap(RecordId recordId) {
        checkState(childName != ZERO_CHILD_NODES);
        Segment segment = recordId.getSegment();
        RecordId childNodesId = segment.readRecordId(recordId.getRecordNumber(), 2 * RECORD_ID_BYTES);
        return reader.readMap(childNodesId);
    }

    public NodeState getChildNode(String name, RecordId recordId) {
        if (childName == ZERO_CHILD_NODES) {
            return MISSING_NODE;
        } else if (childName == MANY_CHILD_NODES) {
            MapRecord map = getChildNodeMap(recordId);
            MapEntry child = map.getEntry(name);
            if (child != null) {
                return child.getNodeState();
            } else {
                return MISSING_NODE;
            }
        } else if (name.equals(childName)) {
            Segment segment = recordId.getSegment();
            RecordId childNodeId = segment.readRecordId(recordId.getRecordNumber(), 2 * RECORD_ID_BYTES);
            return reader.readNode(childNodeId);
        } else {
            return MISSING_NODE;
        }
    }

    Iterable<? extends ChildNodeEntry> getChildNodeEntries(RecordId recordId) {
        if (childName == ZERO_CHILD_NODES) {
            return Collections.emptyList();
        } else if (childName == MANY_CHILD_NODES) {
            MapRecord map = getChildNodeMap(recordId);
            return map.getEntries();
        } else {
            Segment segment = recordId.getSegment();
            RecordId childNodeId = segment.readRecordId(recordId.getRecordNumber(), 2 * RECORD_ID_BYTES);
            return Collections.singletonList(new MemoryChildNodeEntry(
                    childName, reader.readNode(childNodeId)));
        }
    }

    public boolean compare(RecordId thisId, RecordId thatId) {
        checkNotNull(thisId);
        checkNotNull(thatId);

        // Compare properties
        for (int i = 0; i < properties.length; i++) {
            PropertyState thisProperty = getProperty(thisId, i);
            PropertyState thatProperty = getProperty(thatId, i);
            if (!thisProperty.equals(thatProperty)) {
                return false;
            }
        }

        // Compare child nodes
        if (childName == ZERO_CHILD_NODES) {
            return true;
        } else if (childName != MANY_CHILD_NODES) {
            NodeState thisChild = getChildNode(childName, thisId);
            NodeState thatChild = getChildNode(childName, thatId);
            return thisChild.equals(thatChild);
        } else {
            // TODO: Leverage the HAMT data structure for the comparison
            MapRecord thisMap = getChildNodeMap(thisId);
            MapRecord thatMap = getChildNodeMap(thatId);
            if (Record.fastEquals(thisMap, thatMap)) {
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
        return Objects.hashCode(primaryType, mixinTypes,
                Arrays.asList(properties), getTemplateType(), childName);
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
        for (PropertyTemplate property : properties) {
            builder.append(property);
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

    short getTemplateType() {
        if (childName == ZERO_CHILD_NODES) {
            return ZERO_CHILD_NODES_TYPE;
        } else if (childName == MANY_CHILD_NODES) {
            return MANY_CHILD_NODES_TYPE;
        } else {
            return SINGLE_CHILD_NODE_TYPE;
        }
    }

    public int estimateMemoryUsage() {
        int size = OBJECT_HEADER_SIZE;
        size += 48;
        size += StringUtils.estimateMemoryUsage(childName);
        size += estimateMemoryUsage(mixinTypes);
        size += estimateMemoryUsage(primaryType);
        for (PropertyTemplate property : properties) {
            size += property.estimateMemoryUsage();
        }
        return size;
    }

    private static int estimateMemoryUsage(PropertyState propertyState) {
        if (propertyState == null) {
            return 0;
        }
        int size = OBJECT_HEADER_SIZE;
        size += StringUtils.estimateMemoryUsage(propertyState.getName());
        for (int k = 0; k < propertyState.count(); k++) {
            String s = propertyState.getValue(STRING, k);
            size += StringUtils.estimateMemoryUsage(s);
        }
        return size;
    }

}
