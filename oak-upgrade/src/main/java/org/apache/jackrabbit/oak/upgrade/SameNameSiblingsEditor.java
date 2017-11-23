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
package org.apache.jackrabbit.oak.upgrade;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

/**
 * This editor check if same name sibling nodes are allowed under a given
 * parent. If they are not, they will be renamed by replacing brackets with a
 * underscore: {@code sns_name[3] -> sns_name_3_}.
 */
public class SameNameSiblingsEditor extends DefaultEditor {

    private static final Logger logger = LoggerFactory.getLogger(SameNameSiblingsEditor.class);

    private static final Pattern SNS_REGEX = Pattern.compile("^(.+)\\[(\\d+)\\]$");

    private static final Predicate<NodeState> NO_SNS_PROPERTY = new Predicate<NodeState>() {
        @Override
        public boolean apply(NodeState input) {
            return !input.getBoolean(JCR_SAMENAMESIBLINGS);
        }
    };

    /**
     * List of node type definitions that doesn't allow to have SNS children.
     */
    private final List<ChildTypeDef> childrenDefsWithoutSns;

    /**
     * Builder of the current node.
     */
    private final NodeBuilder builder;

    /**
     * Path to the current node.
     */
    private final String path;

    public static class Provider implements EditorProvider {
        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
                throws CommitFailedException {
            return new SameNameSiblingsEditor(builder);
        }
    }

    public SameNameSiblingsEditor(NodeBuilder rootBuilder) {
        this.childrenDefsWithoutSns = prepareChildDefsWithoutSns(rootBuilder.getNodeState());
        this.builder = rootBuilder;
        this.path = "";
    }

    public SameNameSiblingsEditor(SameNameSiblingsEditor parent, String name, NodeBuilder builder) {
        this.childrenDefsWithoutSns = parent.childrenDefsWithoutSns;
        this.builder = builder;
        this.path = new StringBuilder(parent.path).append('/').append(name).toString();
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return new SameNameSiblingsEditor(this, name, builder.getChildNode(name));
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return new SameNameSiblingsEditor(this, name, builder.getChildNode(name));
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (hasSameNamedChildren(after)) {
            renameSameNamedChildren(builder);
        }
    }

    /**
     * Prepare a list of node definitions that doesn't allow having SNS children.
     *
     * @param root Repository root
     * @return a list of node definitions denying SNS children
     */
    private static List<ChildTypeDef> prepareChildDefsWithoutSns(NodeState root) {
        List<ChildTypeDef> defs = new ArrayList<ChildTypeDef>();
        NodeState types = root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);
        for (ChildNodeEntry typeEntry : types.getChildNodeEntries()) {
            NodeState type = typeEntry.getNodeState();
            TypePredicate typePredicate = new TypePredicate(root, typeEntry.getName());
            defs.addAll(parseResidualChildNodeDefs(root, type, typePredicate));
            defs.addAll(parseNamedChildNodeDefs(root, type, typePredicate));
        }
        return defs;
    }

    private static List<ChildTypeDef> parseNamedChildNodeDefs(NodeState root, NodeState parentType,
            TypePredicate parentTypePredicate) {
        List<ChildTypeDef> defs = new ArrayList<ChildTypeDef>();
        NodeState namedChildNodeDefinitions = parentType.getChildNode(REP_NAMED_CHILD_NODE_DEFINITIONS);
        for (ChildNodeEntry childName : namedChildNodeDefinitions.getChildNodeEntries()) {
            for (String childType : filterChildren(childName.getNodeState(), NO_SNS_PROPERTY)) {
                TypePredicate childTypePredicate = new TypePredicate(root, childType);
                defs.add(new ChildTypeDef(parentTypePredicate, childName.getName(), childTypePredicate));
            }
        }
        return defs;
    }

    private static List<ChildTypeDef> parseResidualChildNodeDefs(NodeState root, NodeState parentType,
            TypePredicate parentTypePredicate) {
        List<ChildTypeDef> defs = new ArrayList<ChildTypeDef>();
        NodeState resChildNodeDefinitions = parentType.getChildNode(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
        for (String childType : filterChildren(resChildNodeDefinitions, NO_SNS_PROPERTY)) {
            TypePredicate childTypePredicate = new TypePredicate(root, childType);
            defs.add(new ChildTypeDef(parentTypePredicate, childTypePredicate));
        }
        return defs;
    }

    /**
     * Filter children of the given node using predicate and return the list of matching child names.
     *
     * @param parent
     * @param predicate
     * @return a list of names of children accepting the predicate
     */
    private static Iterable<String> filterChildren(NodeState parent, final Predicate<NodeState> predicate) {
        return transform(filter(parent.getChildNodeEntries(), new Predicate<ChildNodeEntry>() {
            @Override
            public boolean apply(ChildNodeEntry input) {
                return predicate.apply(input.getNodeState());
            }
        }), new Function<ChildNodeEntry, String>() {
            @Override
            public String apply(ChildNodeEntry input) {
                return input.getName();
            }
        });
    }

    /**
     * Check if there are SNS nodes under the given parent.
     *
     * @param parent
     * @return {@code true} if there are SNS children
     */
    private boolean hasSameNamedChildren(NodeState parent) {
        for (String name : parent.getChildNodeNames()) {
            if (SNS_REGEX.matcher(name).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Rename all SNS children which are not allowed under the given parent.
     */
    private void renameSameNamedChildren(NodeBuilder parent) {
        NodeState parentNode = parent.getNodeState();
        Map<String, String> toBeRenamed = new HashMap<String, String>();
        for (String name : parent.getChildNodeNames()) {
            Matcher m = SNS_REGEX.matcher(name);
            if (!m.matches()) {
                continue;
            } else if (isSnsAllowedForChild(parentNode, name)) {
                continue;
            }
            String prefix = m.group(1);
            String index = m.group(2);
            toBeRenamed.put(name, createNewName(parentNode, prefix, index));
        }
        for (Entry<String, String> e : toBeRenamed.entrySet()) {
            logger.warn("Renaming SNS {}/{} to {}", path, e.getKey(), e.getValue());
            parent.getChildNode(e.getKey()).moveTo(parent, e.getValue());
        }
    }

    /**
     * Check if SNS with given name is allowed under the given parent using the {@link #childrenDefsWithoutSns} list.
     */
    private boolean isSnsAllowedForChild(NodeState parent, String name) {
        for (ChildTypeDef snsDef : childrenDefsWithoutSns) {
            if (snsDef.applies(parent, name)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create new name for the conflicting SNS node. This method makes sure that
     * no node with this name already exists.
     *
     * @param prefix prefix of the new name, eg. <b>my_name</b>[3]
     * @param index SNS index, eg. my_name[<b>3</b>]
     * @param parent of the SNS node
     * @return new and unused name for the node
     */
    private String createNewName(NodeState parent, String prefix, String index) {
        String newName;
        int i = 1;
        do {
            if (i == 1) {
                newName = String.format("%s_%s_", prefix, index);
            } else {
                newName = String.format("%s_%s_%d", prefix, index, i);
            }
            i++;
        } while (parent.getChildNode(newName).exists());
        return newName;
    }

    /**
     * Definition of a children type. It contains the parent type, the child
     * type and an optional child name.
     */
    private static class ChildTypeDef {

        private final TypePredicate parentType;

        private final String childNameConstraint;

        private final TypePredicate childType;

        public ChildTypeDef(TypePredicate parentType, String childName, TypePredicate childType) {
            this.parentType = parentType;
            this.childNameConstraint = childName;
            this.childType = childType;
        }

        public ChildTypeDef(TypePredicate parentType, TypePredicate childType) {
            this(parentType, null, childType);
        }

        public boolean applies(NodeState parent, String childName) {
            boolean result = true;
            result &= parentType.apply(parent);
            result &= childNameConstraint == null || childName.startsWith(this.childNameConstraint + '[');
            result &= childType.apply(parent.getChildNode(childName));
            return result;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append(parentType.toString()).append(" > ");
            if (childNameConstraint == null) {
                result.append("*");
            } else {
                result.append(childNameConstraint);
            }
            result.append(childType.toString());
            return result.toString();
        }
    }
}
