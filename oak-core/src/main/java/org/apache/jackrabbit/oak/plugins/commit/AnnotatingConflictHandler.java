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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.ADD_EXISTING;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.CHANGE_CHANGED;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.CHANGE_DELETED;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.DELETE_CHANGED;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.DELETE_DELETED;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_REP_MERGE_CONFLICT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.REP_OURS;

/**
 * This {@link ConflictHandler} implementation resolves conflicts to
 * {@link Resolution#THEIRS} and in addition marks nodes where a conflict
 * occurred with the mixin {@code rep:MergeConflict}:
 *
 * <pre>
 * [rep:MergeConflict]
 *   mixin
 *   primaryitem rep:ours
 *   + rep:ours (nt:unstructured) protected IGNORE
 * </pre>
 *
 * The {@code rep:ours} sub node contains our version of the node prior to
 * the conflict.
 *
 * @see ConflictValidator
 */
public class AnnotatingConflictHandler implements ConflictHandler {
    private final CoreValueFactory valueFactory;

    public AnnotatingConflictHandler(CoreValueFactory valueFactory) {
        this.valueFactory = valueFactory;
    }

    @Override
    public Resolution addExistingProperty(Tree parent, PropertyState ours, PropertyState theirs) {
        Tree marker = addConflictMarker(parent);
        getOrCreateNode(marker, ADD_EXISTING).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeDeletedProperty(Tree parent, PropertyState ours) {
        Tree marker = addConflictMarker(parent);
        getOrCreateNode(marker, CHANGE_DELETED).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeChangedProperty(Tree parent, PropertyState ours, PropertyState theirs) {
        Tree marker = addConflictMarker(parent);
        getOrCreateNode(marker, CHANGE_CHANGED).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteChangedProperty(Tree parent, PropertyState theirs) {
        Tree marker = addConflictMarker(parent);
        getOrCreateNode(marker, DELETE_CHANGED).setProperty(theirs);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteDeletedProperty(Tree parent, PropertyState ours) {
        Tree marker = addConflictMarker(parent);
        getOrCreateNode(marker, DELETE_DELETED).setProperty(ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution addExistingNode(Tree parent, String name, NodeState ours, NodeState theirs) {
        Tree marker = addConflictMarker(parent);
        addChild(getOrCreateNode(marker, ADD_EXISTING), name, ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeDeletedNode(Tree parent, String name, NodeState ours) {
        Tree marker = addConflictMarker(parent);
        addChild(getOrCreateNode(marker, CHANGE_DELETED), name, ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteChangedNode(Tree parent, String name, NodeState theirs) {
        Tree marker = addConflictMarker(parent);
        markChild(getOrCreateNode(marker, DELETE_CHANGED), name);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteDeletedNode(Tree parent, String name) {
        Tree marker = addConflictMarker(parent);
        markChild(getOrCreateNode(marker, DELETE_DELETED), name);
        return Resolution.THEIRS;
    }

    private static Tree addConflictMarker(Tree parent) {
        PropertyState jcrMixin = parent.getProperty(JCR_MIXINTYPES);
        List<String> mixins;
        if (jcrMixin == null) {
            mixins = Lists.newArrayList();
        }
        else {
            mixins = Lists.newArrayList(jcrMixin.getValue(NAMES));
        }
        if (!mixins.contains(MIX_REP_MERGE_CONFLICT)) {
            mixins.add(MIX_REP_MERGE_CONFLICT);
            parent.setProperty(JCR_MIXINTYPES, mixins, NAMES);
        }

        return getOrCreateNode(parent, REP_OURS);
    }

    private static Tree getOrCreateNode(Tree parent, String name) {
        Tree child = parent.getChild(name);
        if (child == null) {
            child = parent.addChild(name);
        }
        return child;
    }

    private static void addChild(Tree parent, String name, NodeState state) {
        Tree child = parent.addChild(name);
        for (PropertyState property : state.getProperties()) {
            child.setProperty(property);
        }
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            addChild(child, entry.getName(), entry.getNodeState());
        }
    }

    private static void markChild(Tree parent, String name) {
        parent.addChild(name);
    }

}
