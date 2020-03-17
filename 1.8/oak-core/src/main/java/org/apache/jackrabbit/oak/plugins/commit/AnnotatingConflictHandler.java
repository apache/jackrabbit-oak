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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_REP_MERGE_CONFLICT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_OURS;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_PROPERTY;

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.state.ConflictType;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This {@link ThreeWayConflictHandler} implementation resolves conflicts to
 * {@link org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler.Resolution#THEIRS} and in addition marks nodes where a
 * conflict occurred with the mixin {@code rep:MergeConflict}:
 *
 * <pre>
 * [rep:MergeConflict]
 *   mixin
 *   primaryitem rep:ours
 *   + rep:ours (rep:Unstructured) protected IGNORE
 * </pre>
 *
 * The {@code rep:ours} sub node contains our version of the node prior to
 * the conflict.
 *
 * @see ConflictValidator
 */
public class AnnotatingConflictHandler implements ThreeWayConflictHandler {

    @Override
    public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, ADD_EXISTING_PROPERTY).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeDeletedProperty(NodeBuilder parent, PropertyState ours, PropertyState base) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, CHANGE_DELETED_PROPERTY).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs,
            PropertyState base) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, CHANGE_CHANGED_PROPERTY).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs, PropertyState base) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, DELETE_CHANGED_PROPERTY).setProperty(theirs);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState base) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, DELETE_DELETED_PROPERTY).setProperty(base);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours, NodeState theirs) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, ADD_EXISTING_NODE).setChildNode(name, ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours, NodeState base) {
        NodeBuilder marker = addConflictMarker(parent);
        createChild(marker, CHANGE_DELETED_NODE).setChildNode(name, ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs, NodeState base) {
        NodeBuilder marker = addConflictMarker(parent);
        markChild(createChild(marker, DELETE_CHANGED_NODE), name);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteDeletedNode(NodeBuilder parent, String name, NodeState base) {
        NodeBuilder marker = addConflictMarker(parent);
        markChild(createChild(marker, DELETE_DELETED_NODE), name);
        return Resolution.THEIRS;
    }

    private static NodeBuilder addConflictMarker(NodeBuilder parent) {
        List<String> mixins = newArrayList(parent.getNames(JCR_MIXINTYPES));
        if (mixins.add(MIX_REP_MERGE_CONFLICT)) {
            parent.setProperty(JCR_MIXINTYPES, mixins, NAMES);
        }
        NodeBuilder repOurs = parent.child(REP_OURS);
        repOurs.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_UNSTRUCTURED, Type.NAME);
        return repOurs;
    }

    private static NodeBuilder createChild(NodeBuilder parent, ConflictType ct) {
        return parent.child(ct.getName());
    }

    private static void markChild(NodeBuilder parent, String name) {
        parent.child(name);
    }

}
