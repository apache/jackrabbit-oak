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
package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff.CONFLICT;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_PROPERTY;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeConstants;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ConflictType;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergingNodeStateDiff... TODO
 */
public final class MergingNodeStateDiff extends DefaultNodeStateDiff {
    private static final Logger LOG = LoggerFactory.getLogger(MergingNodeStateDiff.class);

    private final NodeState parent;
    private final NodeBuilder target;
    private final ConflictHandler conflictHandler;

    private MergingNodeStateDiff(NodeState parent, NodeBuilder target, ConflictHandler conflictHandler) {
        this.parent = parent;
        this.target = target;
        this.conflictHandler = conflictHandler;
    }

    static NodeState merge(NodeState fromState, NodeState toState, ConflictHandler conflictHandler) {
        return merge(fromState, toState, toState.builder(), conflictHandler).getNodeState();
    }

    private static NodeBuilder merge(NodeState fromState, NodeState toState, NodeBuilder target,
                                     ConflictHandler conflictHandler) {
        toState.compareAgainstBaseState(fromState,
                new MergingNodeStateDiff(toState, target, conflictHandler));

        return target;
    }

    //------------------------------------------------------< NodeStateDiff >---

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (CONFLICT.equals(name)) {
            for (ChildNodeEntry conflict : after.getChildNodeEntries()) {
                resolveConflict(ConflictType.fromName(conflict.getName()), conflict.getNodeState());
            }

            target.getChildNode(CONFLICT).remove();
        }
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        merge(before, after, target.child(name), conflictHandler);
        return true;
    }

    //------------------------------------------------------------< private >---

    private void resolveConflict(ConflictType conflictType, NodeState conflictInfo) {
        PropertyConflictHandler propertyConflictHandler = propertyConflictHandlers.get(conflictType);
        if (propertyConflictHandler != null) {
            for (PropertyState ours : conflictInfo.getProperties()) {
                PropertyState theirs = parent.getProperty(ours.getName());
                Resolution resolution = propertyConflictHandler.resolve(ours, theirs);
                applyResolution(resolution, conflictType, ours);
            }
        }
        else {
            NodeConflictHandler nodeConflictHandler = nodeConflictHandlers.get(conflictType);
            if (nodeConflictHandler != null) {
                for (ChildNodeEntry oursCNE : conflictInfo.getChildNodeEntries()) {
                    String name = oursCNE.getName();
                    NodeState ours = oursCNE.getNodeState();
                    NodeState theirs = parent.getChildNode(name);
                    Resolution resolution = nodeConflictHandler.resolve(name, ours, theirs);
                    applyResolution(resolution, conflictType, name, ours);
                    if (LOG.isDebugEnabled()) {
                        String diff = JsopDiff.diffToJsop(ours, theirs);
                        LOG.debug(
                                "{} resolved conflict of type {} with resolution {} on node {}, conflict trace {}",
                                nodeConflictHandler, conflictType, resolution,
                                name, diff);
                    }
                }
            }
            else {
                LOG.warn("Ignoring unknown conflict '" + conflictType + '\'');
            }
        }

        NodeBuilder conflictMarker = getConflictMarker(conflictType);
        if (conflictMarker != null) {
            assert conflictMarker.getChildNodeCount(1) == 0;
        }
    }

    private void applyResolution(Resolution resolution, ConflictType conflictType, PropertyState ours) {
        String name = ours.getName();
        NodeBuilder conflictMarker = getConflictMarker(conflictType);
        if (resolution == Resolution.OURS) {
            if (DELETE_CHANGED_PROPERTY == conflictType) {
                target.removeProperty(name);
            }
            else {
                target.setProperty(ours);
            }

        }
        conflictMarker.removeProperty(name);
    }

    private void applyResolution(Resolution resolution, ConflictType conflictType, String name, NodeState ours) {
        NodeBuilder conflictMarker = getConflictMarker(conflictType);
        if (resolution == Resolution.OURS) {
            if (DELETE_CHANGED_NODE == conflictType) {
                removeChild(target, name);
            }
            else {
                addChild(target, name, ours);
            }
        }
        conflictMarker.getChildNode(name).remove();
    }

    private NodeBuilder getConflictMarker(ConflictType conflictType) {
        final String conflictName = conflictType.getName();
        if (target.hasChildNode(CONFLICT)) {
            NodeBuilder conflict = target.child(CONFLICT);
            if (conflict.hasChildNode(conflictName)) {
                return conflict.child(conflictName);
            }
        }

        return null;
    }

    private interface PropertyConflictHandler {
        Resolution resolve(PropertyState ours, PropertyState theirs);
    }

    private interface NodeConflictHandler {
        Resolution resolve(String name, NodeState ours, NodeState theirs);
    }

    private final Map<ConflictType, PropertyConflictHandler> propertyConflictHandlers = ImmutableMap.of(
        ADD_EXISTING_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.addExistingProperty(target, ours, theirs);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<ADD_EXISTING_PROPERTY>";
            }
        },
        CHANGE_DELETED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.changeDeletedProperty(target, ours);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<CHANGE_DELETED_PROPERTY>";
            }
        },
        CHANGE_CHANGED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.changeChangedProperty(target, ours, theirs);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<CHANGE_CHANGED_PROPERTY>";
            }
        },
        DELETE_DELETED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.deleteDeletedProperty(target, ours);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<DELETE_DELETED_PROPERTY>";
            }
        },
        DELETE_CHANGED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.deleteChangedProperty(target, theirs);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<DELETE_CHANGED_PROPERTY>";
            }
        }
    );

    private final Map<ConflictType, NodeConflictHandler> nodeConflictHandlers = ImmutableMap.of(
        ADD_EXISTING_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.addExistingNode(target, name, ours, theirs);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<ADD_EXISTING_NODE>";
            }
        },
        CHANGE_DELETED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.changeDeletedNode(target, name, ours);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<CHANGE_DELETED_NODE>";
            }
        },
        DELETE_CHANGED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.deleteChangedNode(target, name, theirs);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<DELETE_CHANGED_NODE>";
            }
        },
        DELETE_DELETED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.deleteDeletedNode(target, name);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<DELETE_DELETED_NODE>";
            }
        }
    );

    private static void addChild(NodeBuilder target, String name, NodeState state) {
        target.setChildNode(name, state);
        PropertyState childOrder = target.getProperty(TreeConstants.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = PropertyBuilder.copy(NAME, childOrder);
            builder.addValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

    private static void removeChild(NodeBuilder target, String name) {
        target.getChildNode(name).remove();
        PropertyState childOrder = target.getProperty(TreeConstants.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = PropertyBuilder.copy(NAME, childOrder);
            builder.removeValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

}
