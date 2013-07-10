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
package org.apache.jackrabbit.oak.plugins.index.property;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.encode;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PRIMARY_SUBTYPES;

import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Index editor for keeping a property index up to date.
 * 
 * @see PropertyIndex
 * @see PropertyIndexLookup
 */
class PropertyIndexEditor implements IndexEditor {

    /** Index storage strategy */
    private static final IndexStoreStrategy STORE =
            new ContentMirrorStoreStrategy();

    /** Parent editor, or {@code null} if this is the root editor. */
    private final PropertyIndexEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /** Index definition node builder */
    private final NodeBuilder definition;

    private final Set<String> propertyNames;

    private final Set<String> primaryTypes;

    private final Set<String> mixinTypes;

    private final Set<String> keysToCheckForUniqueness;

    /**
     * Flag to indicate whether individual property changes should
     * be tracked for this node.
     */
    private boolean trackChanges;

    /**
     * Matching property value keys from the before state,
     * or {@code null} if this node is not indexed.
     */
    private Set<String> beforeKeys;

    /**
     * Matching property value keys from the after state,
     * or {@code null} if this node is not indexed.
     */
    private Set<String> afterKeys;

    public PropertyIndexEditor(NodeBuilder definition, NodeState root) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;

        // get property names
        this.propertyNames = newHashSet(definition.getNames(PROPERTY_NAMES));

        // get declaring types, and all their subtypes
        // TODO: should we reindex when type definitions change?
        if (definition.hasProperty(DECLARING_NODE_TYPES)) {
            this.primaryTypes = newHashSet();
            this.mixinTypes = newHashSet();
            NodeState types =
                    root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);
            for (String name : definition.getNames(DECLARING_NODE_TYPES)) {
                NodeState type = types.getChildNode(name);
                if (type.getBoolean(JCR_ISMIXIN)) {
                    mixinTypes.add(name);
                } else {
                    primaryTypes.add(name);
                }
                addAll(mixinTypes, type.getNames(OAK_MIXIN_SUBTYPES));
                addAll(primaryTypes, type.getNames(OAK_PRIMARY_SUBTYPES));
            }
        } else {
            this.primaryTypes = null;
            this.mixinTypes = null;
        }

        // keep track of modified keys for uniqueness checks
        if (definition.getBoolean(IndexConstants.UNIQUE_PROPERTY_NAME)) {
            this.keysToCheckForUniqueness = newHashSet();
        } else {
            this.keysToCheckForUniqueness = null;
        }
    }

    private PropertyIndexEditor(PropertyIndexEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.propertyNames = parent.propertyNames;
        this.primaryTypes = parent.primaryTypes;
        this.mixinTypes = parent.mixinTypes;
        this.keysToCheckForUniqueness = parent.keysToCheckForUniqueness;
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    private String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    private boolean isOfMatchingType(NodeState state) {
        // no type limitations
        return primaryTypes == null 
                || primaryTypes.contains(state.getName(JCR_PRIMARYTYPE))
                || any(state.getNames(JCR_MIXINTYPES), in(mixinTypes));
    }

    private static void addValueKeys(Set<String> keys, PropertyState property) {
        if (property.getType().tag() != PropertyType.BINARY) {
            keys.addAll(encode(PropertyValues.create(property)));
        }
    }

    private static void addMatchingKeys(
            Set<String> keys, NodeState state, Iterable<String> propertyNames) {
        for (String propertyName : propertyNames) {
            PropertyState property = state.getProperty(propertyName);
            if (property != null) {
                addValueKeys(keys, property);
            }
        }
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        boolean beforeMatches = before.exists() && isOfMatchingType(before);
        boolean afterMatches  = after.exists()  && isOfMatchingType(after);

        if (beforeMatches || afterMatches) {
            beforeKeys = newHashSet();
            afterKeys = newHashSet();
        }
 
        if (beforeMatches && afterMatches) {
            trackChanges = true;
        } else if (beforeMatches) {
            // all matching values should be removed from the index
            addMatchingKeys(beforeKeys, before, propertyNames);
        } else if (afterMatches) {
            // all matching values should be added to the index
            addMatchingKeys(afterKeys, after, propertyNames);
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (beforeKeys != null) {
            Set<String> sharedKeys = newHashSet(beforeKeys);
            sharedKeys.retainAll(afterKeys);

            beforeKeys.removeAll(sharedKeys);
            afterKeys.removeAll(sharedKeys);

            if (!beforeKeys.isEmpty() || !afterKeys.isEmpty()) {
                NodeBuilder index = definition.child(INDEX_CONTENT_NODE_NAME);
                STORE.update(index, getPath(), beforeKeys, afterKeys);

                if (keysToCheckForUniqueness != null) {
                    keysToCheckForUniqueness.addAll(afterKeys);
                }
            }
        }

        if (parent == null) {
            // make sure that the index node exist, even with no content
            NodeBuilder index = definition.child(INDEX_CONTENT_NODE_NAME);

            // check uniqueness constraints when leaving the root
            if (keysToCheckForUniqueness != null
                    && !keysToCheckForUniqueness.isEmpty()) {
                NodeState state = index.getNodeState();
                for (String key : keysToCheckForUniqueness) {
                    if (STORE.count(state, singleton(key), 2) > 1) {
                        throw new CommitFailedException(
                                CONSTRAINT, 30,
                                "Uniqueness constraint violated for key " + key);
                    }
                }
            }
        }
    }

    @Override
    public void propertyAdded(PropertyState after) {
        if (trackChanges && propertyNames.contains(after.getName())) {
            addValueKeys(afterKeys, after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        if (trackChanges && propertyNames.contains(after.getName())) {
            addValueKeys(beforeKeys, before);
            addValueKeys(afterKeys, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        if (trackChanges && propertyNames.contains(before.getName())) {
            addValueKeys(beforeKeys, before);
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        return new PropertyIndexEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after) {
        return new PropertyIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) {
        return new PropertyIndexEditor(this, name);
    }

}
