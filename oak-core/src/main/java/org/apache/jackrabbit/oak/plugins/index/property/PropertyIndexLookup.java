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

import static com.google.common.collect.Iterables.contains;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.encode;

import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Is responsible for querying the property index content.
 * <br>
 * This class can be used directly on a subtree where there is an index defined
 * by supplying a {@link NodeState} root.
 * 
 * <pre>
 * <code>
 * {
 *     NodeState state = ... // get a node state
 *     PropertyIndexLookup lookup = new PropertyIndexLookup(state);
 *     Set<String> hits = lookup.find("foo", PropertyValues.newString("xyz"));
 * }
 * </code>
 * </pre>
 */
public class PropertyIndexLookup {

    private static final int MAX_COST = 100;

    private final IndexStoreStrategy store = new ContentMirrorStoreStrategy();

    private final NodeState root;

    public PropertyIndexLookup(NodeState root) {
        this.root = root;
    }

    /**
     * Checks whether the named property is indexed somewhere along the given
     * path. Lookup starts at the current path (at the root of this object) and
     * traverses down the path.
     * 
     * @param propertyName property name
     * @param path lookup path
     * @param filter for the node type restriction (null if no node type restriction)
     * @return true if the property is indexed
     */
    public boolean isIndexed(String propertyName, String path, Filter filter) {
        Set<String> supertypes = null;
        if (filter != null && !filter.matchesAllTypes()) {
            supertypes = filter.getSupertypes();
        }

        if (PathUtils.denotesRoot(path)) {
            return getIndexDataNode(root, propertyName, supertypes) != null;
        }

        NodeState node = root;
        Iterator<String> it = PathUtils.elements(path).iterator();
        while (it.hasNext()) {
            if (getIndexDataNode(node, propertyName, supertypes) != null) {
                return true;
            }
            node = node.getChildNode(it.next());
        }
        return false;
    }

    public Iterable<String> query(Filter filter, String propertyName, PropertyValue value) {
        Set<String> supertypes = null;
        if (filter != null && !filter.matchesAllTypes()) {
            supertypes = filter.getSupertypes();
        }

        NodeState state = getIndexDataNode(root, propertyName, supertypes);
        if (state == null) {
            throw new IllegalArgumentException("No index for " + propertyName);
        }
        return store.query(filter, propertyName, state, encode(value));
    }

    public double getCost(Filter filter, String name, PropertyValue value) {
        Set<String> supertypes = null;
        if (filter != null && !filter.matchesAllTypes()) {
            supertypes = filter.getSupertypes();
        }

        NodeState state = getIndexDataNode(root, name, supertypes);
        if (state == null) {
            return Double.POSITIVE_INFINITY;
        }
        return store.count(state, encode(value), MAX_COST);
    }

    /**
     * Get the node with the index data for the given property, if there is an
     * applicable index with data.
     * 
     * @param propertyName the property name
     * @param supertypes the filter node type and all its supertypes,
     *                   or {@code null} if the filter matches all types
     * @return the node where the index data is stored, or null if no index
     *         definition or index data node was found
     */
    @Nullable
    private NodeState getIndexDataNode(
            NodeState node, String propertyName, Set<String> supertypes) {
        //keep a fallback to a matching index def that has *no* node type constraints
        NodeState fallback = null;

        NodeState state = node.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            NodeState ns = entry.getNodeState();
            PropertyState type = ns.getProperty(TYPE_PROPERTY_NAME);
            if (type == null || type.isArray() || !TYPE.equals(type.getValue(Type.STRING))) {
                continue;
            }
            if (contains(ns.getNames(PROPERTY_NAMES), propertyName)) {
                NodeState index = ns.getChildNode(INDEX_CONTENT_NODE_NAME);
                if (ns.hasProperty(DECLARING_NODE_TYPES)) {
                    if (supertypes != null) {
                        for (String typeName : ns.getNames(DECLARING_NODE_TYPES)) {
                            if (supertypes.contains(typeName)) {
                                // TODO: prefer the most specific type restriction
                                return index;
                            }
                        }
                    }
                } else if (supertypes == null) {
                    return index;
                } else if (fallback == null) {
                    fallback = index;
                }
            }
        }
        return fallback;
    }

}
