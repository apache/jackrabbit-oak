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
package org.apache.jackrabbit.oak.plugins.value;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.ConflictHandler;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.Iterators;

import static org.apache.jackrabbit.oak.util.Iterators.toList;

/**
 * This {@link ConflictHandler} implementation resolves conflicts to
 * {@link Resolution#THEIRS} and in addition marks nodes where a conflict
 * occurred with {@code mix:mergeConflict}:
 *
 * <pre>
 * [mix:mergeConflict]
 *   mixin
 *   primaryitem jcr:ours
 *   + jcr:ours (nt:unstructured)
 * </pre>
 *
 * The {@code jcr:ours} sub node contains our version of the node prior to
 * the conflict.
 *
 * @see ConflictValidator
 */
public class AnnotatingConflictHandler implements ConflictHandler {
    private static final String JCR_MIXIN_TYPES = "jcr:mixinTypes";

    // TODO: move these constants to some common location for repository internal node types
    private static final String MIX_MERGE_CONFLICT = "mix:mergeConflict";
    private static final String JCR_OURS = "jcr:ours";
    private static final String ADD_EXISTING = "addExisting";
    private static final String CHANGE_DELETED = "changeDeleted";
    private static final String CHANGE_CHANGED = "changeChanged";
    private static final String DELETE_CHANGED = "deleteChanged";
    private static final String DELETE_DELETED = "deleteDeleted";

    private final CoreValueFactory valueFactory;

    public AnnotatingConflictHandler(CoreValueFactory valueFactory) {
        this.valueFactory = valueFactory;
    }

    @Override
    public Resolution addExistingProperty(Tree parent, PropertyState ours, PropertyState theirs) {
        Tree marker = addConflictMarker(parent);
        setProperty(getOrCreateNode(marker, ADD_EXISTING), ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeDeletedProperty(Tree parent, PropertyState ours) {
        Tree marker = addConflictMarker(parent);
        setProperty(getOrCreateNode(marker, CHANGE_DELETED), ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution changeChangedProperty(Tree parent, PropertyState ours, PropertyState theirs) {
        Tree marker = addConflictMarker(parent);
        setProperty(getOrCreateNode(marker, CHANGE_CHANGED), ours);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteChangedProperty(Tree parent, PropertyState theirs) {
        Tree marker = addConflictMarker(parent);
        setProperty(getOrCreateNode(marker, DELETE_CHANGED), theirs);
        return Resolution.THEIRS;
    }

    @Override
    public Resolution deleteDeletedProperty(Tree parent, PropertyState ours) {
        Tree marker = addConflictMarker(parent);
        setProperty(getOrCreateNode(marker, DELETE_DELETED), ours);
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

    private Tree addConflictMarker(Tree parent) {
        PropertyState jcrMixin = parent.getProperty(JCR_MIXIN_TYPES);
        List<CoreValue> mixins = new ArrayList<CoreValue>();
        if (jcrMixin != null) {
            assert jcrMixin.isArray();
            mixins = Iterators.toList(jcrMixin.getValues(), mixins);
        }
        if (!mixins.contains(MIX_MERGE_CONFLICT)) {
            mixins.add(valueFactory.createValue(MIX_MERGE_CONFLICT, PropertyType.NAME));
            parent.setProperty(JCR_MIXIN_TYPES, mixins);
        }

        return getOrCreateNode(parent, JCR_OURS);
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
            setProperty(child, property);
        }
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            addChild(child, entry.getName(), entry.getNodeState());
        }
    }

    private static void markChild(Tree parent, String name) {
        parent.addChild(name);
    }

    private static void setProperty(Tree parent, PropertyState property) {
        if (property.isArray()) {
            parent.setProperty(property.getName(),
                    toList(property.getValues(), new ArrayList<CoreValue>()));
        }
        else {
            parent.setProperty(property.getName(), property.getValue());
        }
    }

}
