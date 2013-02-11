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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergingNodeStateDiff... TODO
 */
public final class MergingNodeStateDiff extends DefaultNodeStateDiff {
    private static final Logger LOG = LoggerFactory.getLogger(MergingNodeStateDiff.class);

    public static final String CONFLICT = ":conflict";
    public static final String DELETE_CHANGED_PROPERTY = "deleteChangedProperty";
    public static final String DELETE_CHANGED_NODE = "deleteChangedNode";
    public static final String ADD_EXISTING_PROPERTY = "addExistingProperty";
    public static final String CHANGE_DELETED_PROPERTY = "changeDeletedProperty";
    public static final String CHANGE_CHANGED_PROPERTY = "changeChangedProperty";
    public static final String DELETE_DELETED_PROPERTY = "deleteDeletedProperty";
    public static final String ADD_EXISTING_NODE = "addExistingNode";
    public static final String CHANGE_DELETED_NODE = "changeDeletedNode";
    public static final String DELETE_DELETED_NODE = "deleteDeletedNode";

    private final NodeState parent;
    private final NodeBuilder target;
    private final ConflictHandler conflictHandler;

    private MergingNodeStateDiff(NodeState parent, NodeBuilder target, ConflictHandler conflictHandler) {
        this.parent = parent;
        this.target = target;
        this.conflictHandler = conflictHandler;
    }

    static NodeState merge(NodeState fromState, NodeState toState, ConflictHandler conflictHandler) {
        return merge(fromState, toState, toState.builder(), conflictHandler);
    }

    private static NodeState merge(NodeState fromState, NodeState toState, NodeBuilder target,
            ConflictHandler conflictHandler) {
        toState.compareAgainstBaseState(fromState,
                new MergingNodeStateDiff(toState, target, conflictHandler));

        return target.getNodeState();
    }

    //------------------------------------------------------< NodeStateDiff >---

    @Override
    public void childNodeAdded(String name, NodeState after) {
        if (CONFLICT.equals(name)) {
            for (ChildNodeEntry conflict : after.getChildNodeEntries()) {
                resolveConflict(conflict.getName(), conflict.getNodeState());
            }

            target.removeNode(CONFLICT);
        }
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        merge(before, after, target.child(name), conflictHandler);
    }

    //------------------------------------------------------------< private >---

    private void resolveConflict(String conflictName, NodeState conflictInfo) {
        PropertyConflictHandler propertyConflictHandler = propertyConflictHandlers.get(conflictName);
        if (propertyConflictHandler != null) {
            for (PropertyState ours : conflictInfo.getProperties()) {
                PropertyState theirs = parent.getProperty(ours.getName());
                Resolution resolution = propertyConflictHandler.resolve(ours, theirs);
                applyResolution(resolution, conflictName, ours);
            }
        }
        else {
            NodeConflictHandler nodeConflictHandler = nodeConflictHandlers.get(conflictName);
            if (nodeConflictHandler != null) {
                for (ChildNodeEntry oursCNE : conflictInfo.getChildNodeEntries()) {
                    String name = oursCNE.getName();
                    NodeState ours = oursCNE.getNodeState();
                    NodeState theirs = parent.getChildNode(name);
                    Resolution resolution = nodeConflictHandler.resolve(name, ours, theirs);
                    applyResolution(resolution, conflictName, name, ours);
                }
            }
            else {
                LOG.warn("Ignoring unknown conflict '" + conflictName + '\'');
            }
        }

        NodeBuilder conflictMarker = getConflictMarker(conflictName);
        if (conflictMarker != null) {
            assert conflictMarker.getChildNodeCount() == 0;
        }
    }

    private void applyResolution(Resolution resolution, String conflictName, PropertyState ours) {
        String name = ours.getName();
        NodeBuilder conflictMarker = getConflictMarker(conflictName);
        if (resolution == Resolution.OURS) {
            if (DELETE_CHANGED_PROPERTY.equals(conflictName)) {
                target.removeProperty(name);
            }
            else {
                target.setProperty(ours);
            }

        }
        conflictMarker.removeProperty(name);
    }

    private void applyResolution(Resolution resolution, String conflictName, String name, NodeState ours) {
        NodeBuilder conflictMarker = getConflictMarker(conflictName);
        if (resolution == Resolution.OURS) {
            if (DELETE_CHANGED_NODE.equals(conflictName)) {
                removeChild(target, name);
            }
            else {
                addChild(target, name, ours);
            }
        }
        conflictMarker.removeNode(name);
    }

    private NodeBuilder getConflictMarker(String conflictName) {
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

    private final Map<String, PropertyConflictHandler> propertyConflictHandlers = ImmutableMap.of(
        ADD_EXISTING_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.addExistingProperty(target, ours, theirs);
            }
        },
            CHANGE_DELETED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.changeDeletedProperty(target, ours);
            }
        },
        CHANGE_CHANGED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.changeChangedProperty(target, ours, theirs);
            }
        },
            DELETE_DELETED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.deleteDeletedProperty(target, ours);
            }
        },
            DELETE_CHANGED_PROPERTY, new PropertyConflictHandler() {
            @Override
            public Resolution resolve(PropertyState ours, PropertyState theirs) {
                return conflictHandler.deleteChangedProperty(target, theirs);
            }
        }
    );

    private final Map<String, NodeConflictHandler> nodeConflictHandlers = ImmutableMap.of(
        ADD_EXISTING_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.addExistingNode(target, name, ours, theirs);
            }
        },
            CHANGE_DELETED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.changeDeletedNode(target, name, ours);
            }
        },
            DELETE_CHANGED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.deleteChangedNode(target, name, theirs);
            }
        },
            DELETE_DELETED_NODE, new NodeConflictHandler() {
            @Override
            public Resolution resolve(String name, NodeState ours, NodeState theirs) {
                return conflictHandler.deleteDeletedNode(target, name);
            }
        }
    );

    private static void addChild(NodeBuilder target, String name, NodeState state) {
        target.setNode(name, state);
        PropertyState childOrder = target.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.addValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

    private static void removeChild(NodeBuilder target, String name) {
        target.removeNode(name);
        PropertyState childOrder = target.getProperty(TreeImpl.OAK_CHILD_ORDER);
        if (childOrder != null) {
            PropertyBuilder<String> builder = MemoryPropertyBuilder.copy(Type.STRING, childOrder);
            builder.removeValue(name);
            target.setProperty(builder.getPropertyState());
        }
    }

}
