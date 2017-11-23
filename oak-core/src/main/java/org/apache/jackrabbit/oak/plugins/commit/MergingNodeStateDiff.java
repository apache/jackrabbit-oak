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
import static org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff.BASE;
import static org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff.CONFLICT;
import static org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff.OURS;
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
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
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
    private final ThreeWayConflictHandler conflictHandler;

    private MergingNodeStateDiff(NodeState parent, NodeBuilder target, ThreeWayConflictHandler conflictHandler) {
        this.parent = parent;
        this.target = target;
        this.conflictHandler = conflictHandler;
    }

    static NodeState merge(NodeState fromState, NodeState toState, ThreeWayConflictHandler conflictHandler) {
        return merge(fromState, toState, toState.builder(), conflictHandler).getNodeState();
    }

    private static NodeBuilder merge(NodeState fromState, NodeState toState, NodeBuilder target,
                    ThreeWayConflictHandler conflictHandler) {
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
            NodeState oursNS = conflictInfo.getChildNode(OURS);
            NodeState baseNS = conflictInfo.getChildNode(BASE);

            Set<String> processed = Sets.newHashSet();
            for (PropertyState ours : oursNS.getProperties()) {
                String name = ours.getName();
                processed.add(name);
                PropertyState base = baseNS.getProperty(name);
                PropertyState theirs = parent.getProperty(name);
                Resolution resolution = propertyConflictHandler.resolve(ours, theirs, base);
                applyPropertyResolution(resolution, conflictType, name, ours);
            }
            for (PropertyState base : baseNS.getProperties()) {
                String name = base.getName();
                if (processed.contains(name)) {
                    continue;
                }
                PropertyState theirs = parent.getProperty(name);
                Resolution resolution = propertyConflictHandler.resolve(null, theirs, base);
                applyPropertyResolution(resolution, conflictType, name, null);
            }
        } else {
            NodeConflictHandler nodeConflictHandler = nodeConflictHandlers.get(conflictType);
            if (nodeConflictHandler != null) {
                NodeState oursNS = conflictInfo.getChildNode(OURS);
                NodeState baseNS = conflictInfo.getChildNode(BASE);

                Set<String> candidates = Sets.union(Sets.newHashSet(oursNS.getChildNodeNames()),
                        Sets.newHashSet(baseNS.getChildNodeNames()));
                for (String name : candidates) {
                    NodeState ours = oursNS.getChildNode(name);
                    NodeState base = baseNS.getChildNode(name);
                    NodeState theirs = parent.getChildNode(name);
                    Resolution resolution = nodeConflictHandler.resolve(name, ours, theirs, base);
                    applyResolution(resolution, conflictType, name, ours);

                    if (LOG.isDebugEnabled()) {
                        String diff = JsopDiff.diffToJsop(base, theirs);
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
            assert conflictMarker.getChildNode(ConflictAnnotatingRebaseDiff.BASE).getChildNodeCount(1) == 0;
            assert conflictMarker.getChildNode(ConflictAnnotatingRebaseDiff.OURS).getChildNodeCount(1) == 0;
        }
    }

    private void applyPropertyResolution(Resolution resolution, ConflictType conflictType, String name, PropertyState ours) {
        NodeBuilder conflictMarker = getConflictMarker(conflictType);
        if (resolution == Resolution.OURS) {
            if (DELETE_CHANGED_PROPERTY == conflictType || DELETE_DELETED_PROPERTY == conflictType) {
                target.removeProperty(name);
            } else {
                target.setProperty(ours);
            }
        }
        NodeBuilder baseClean = conflictMarker.getChildNode(ConflictAnnotatingRebaseDiff.BASE);
        if (baseClean.exists()) {
            baseClean.removeProperty(name);
        }
        NodeBuilder oursClean = conflictMarker.getChildNode(ConflictAnnotatingRebaseDiff.OURS);
        if (oursClean.exists()) {
            oursClean.removeProperty(name);
        }
    }

    private void applyResolution(Resolution resolution, ConflictType conflictType, String name, NodeState ours) {
        NodeBuilder conflictMarker = getConflictMarker(conflictType);
        if (resolution == Resolution.OURS) {
            if (DELETE_CHANGED_NODE == conflictType || DELETE_DELETED_NODE == conflictType) {
                removeChild(target, name);
            } else {
                addChild(target, name, ours);
            }
        }

        NodeBuilder baseClean = conflictMarker.getChildNode(ConflictAnnotatingRebaseDiff.BASE);
        if (baseClean.exists()) {
            baseClean.getChildNode(name).remove();
        }

        NodeBuilder oursClean = conflictMarker.getChildNode(ConflictAnnotatingRebaseDiff.OURS);
        if (oursClean.exists()) {
            oursClean.getChildNode(name).remove();
        }

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
        Resolution resolve(PropertyState ours, PropertyState theirs, PropertyState base);
    }

    private interface NodeConflictHandler {
        Resolution resolve(String name, NodeState ours, NodeState theirs, NodeState base);
    }

    private final Map<ConflictType, PropertyConflictHandler> propertyConflictHandlers = ImmutableMap.of(
        ADD_EXISTING_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs, PropertyState base) {
                return conflictHandler.addExistingProperty(target, ours, theirs);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<ADD_EXISTING_PROPERTY>";
            }
        },
        CHANGE_DELETED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs, PropertyState base) {
                return conflictHandler.changeDeletedProperty(target, ours, base);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<CHANGE_DELETED_PROPERTY>";
            }
        },
        CHANGE_CHANGED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs, PropertyState base) {
                return conflictHandler.changeChangedProperty(target, ours, theirs, base);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<CHANGE_CHANGED_PROPERTY>";
            }
        },
        DELETE_DELETED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs, PropertyState base) {
                return conflictHandler.deleteDeletedProperty(target, base);
            }

            @Override
            public String toString() {
                return "PropertyConflictHandler<DELETE_DELETED_PROPERTY>";
            }
        },
        DELETE_CHANGED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs, PropertyState base) {
                return conflictHandler.deleteChangedProperty(target, theirs, base);
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
            public Resolution resolve(String name, NodeState ours, NodeState theirs, NodeState base) {
                return conflictHandler.addExistingNode(target, name, ours, theirs);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<ADD_EXISTING_NODE>";
            }
        },
        CHANGE_DELETED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs, NodeState base) {
                return conflictHandler.changeDeletedNode(target, name, ours, base);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<CHANGE_DELETED_NODE>";
            }
        },
        DELETE_CHANGED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs, NodeState base) {
                return conflictHandler.deleteChangedNode(target, name, theirs, base);
            }

            @Override
            public String toString() {
                return "NodeConflictHandler<DELETE_CHANGED_NODE>";
            }
        },
        DELETE_DELETED_NODE, new NodeConflictHandler() {
            @Override
                public Resolution resolve(String name, NodeState ours, NodeState theirs, NodeState base) {
                return conflictHandler.deleteDeletedNode(target, name, base);
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
