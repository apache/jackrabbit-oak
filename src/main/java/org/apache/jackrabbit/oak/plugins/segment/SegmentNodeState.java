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
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
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
        } else {
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
        return AbstractNodeState.getBoolean(this, name);
    }

    @Override
    public long getLong(String name) {
        return AbstractNodeState.getLong(this, name);
    }

    @Override
    public String getString(String name) {
        return AbstractNodeState.getString(this, name);
    }

    @Override
    public Iterable<String> getStrings(String name) {
        return AbstractNodeState.getStrings(this, name);
    }

    @Override
    public String getName(String name) {
        return AbstractNodeState.getName(this, name);
    }

    @Override
    public Iterable<String> getNames(String name) {
        return AbstractNodeState.getNames(this, name);
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
    public SegmentRootBuilder builder() {
        return new SegmentRootBuilder(this, getStore().getWriter());
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base == this) {
             return true; // no changes
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return getTemplate().compareAgainstEmptyState(
                    getSegment(), getRecordId(), diff);
        } else if (base instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) base;
            return getRecordId().equals(that.getRecordId())
                || getTemplate().compareAgainstBaseState(
                        getSegment(), getRecordId(), that.getTemplate(),
                        that.getSegment(), that.getRecordId(), diff);
        } else {
            // fallback
            return AbstractNodeState.compareAgainstBaseState(this, base, diff);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) object;
            if (getRecordId().equals(that.getRecordId())
                    && getStore() == that.getStore()) {
                return true;
            } else {
                Template template = getTemplate();
                return template.equals(that.getTemplate())
                        && template.compare(
                                getSegment(), getRecordId(),
                                that.getSegment(), that.getRecordId());
            }
        } else if (object instanceof NodeState){
            return AbstractNodeState.equals(this, (NodeState) object); // TODO
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return AbstractNodeState.toString(this);
    }

}
