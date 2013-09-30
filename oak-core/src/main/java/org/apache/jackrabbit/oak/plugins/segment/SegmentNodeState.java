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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.Collections;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Objects;

public class SegmentNodeState extends Record implements NodeState {

    static boolean fastEquals(NodeState a, NodeState b) {
        return a instanceof SegmentNodeState
                && b instanceof SegmentNodeState
                && Objects.equal(
                        ((SegmentNodeState) a).getRecordId(),
                        ((SegmentNodeState) b).getRecordId());
    }

    private RecordId templateId = null;

    private Template template = null;

    public SegmentNodeState(Segment segment, RecordId id) {
        super(segment, id);
    }

    RecordId getTemplateId() {
        getTemplate(); // force loading of the template
        return templateId;
    }

    synchronized Template getTemplate() {
        if (template == null) {
            Segment segment = getSegment();
            templateId = segment.readRecordId(getOffset(0));
            template = segment.readTemplate(templateId);
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
        return getTemplate().getPropertyCount();
    }

    @Override
    public boolean hasProperty(String name) {
        checkNotNull(name);
        return getTemplate().hasProperty(name);
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        checkNotNull(name);
        return getTemplate().getProperty(name, getSegment(), getRecordId());
    }

    @Override @Nonnull
    public Iterable<PropertyState> getProperties() {
        return getTemplate().getProperties(getSegment(), getRecordId());
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
            RecordId childNodeId = getChildNodeMap().getEntry(name);
            if (childNodeId != null) {
                return new SegmentNodeState(getSegment(), childNodeId);
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
    public NodeBuilder builder() {
        // TODO: avoid the Segment.store reference
        return new SegmentRootBuilder(this, getSegment().store.getWriter());
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
                        getSegment(), getRecordId(),
                        that.getTemplate(), that.getRecordId(),
                        diff);
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
            if (getRecordId().equals(that.getRecordId())) {
                return true;
            } else {
                Template template = getTemplate();
                return template.equals(that.getTemplate())
                        && template.compare(
                                getSegment(), getRecordId(),
                                that.getSegment(), that.getRecordId());
            }
        } else {
            return super.equals(object); // TODO
        }
    }

}
