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
package org.apache.jackrabbit.oak.plugins.index.p2;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Sets;

/**
 * Is responsible for querying the property index content.
 * 
 * <p>
 * This class can be used directly on a subtree where there is an index defined
 * by supplying a {@link NodeState} root.
 * </p>
 * 
 * <pre>
 * <code>
 * {
 *     NodeState state = ... // get a node state
 *     Property2IndexLookup lookup = new Property2IndexLookup(state);
 *     Set<String> hits = lookup.find("foo", PropertyValues.newString("xyz"));
 * }
 * </code>
 * </pre>
 */
public class Property2IndexLookup {

    private final IndexStoreStrategy store = new ContentMirrorStoreStrategy();

    private final NodeState root;

    public Property2IndexLookup(NodeState root) {
        this.root = root;
    }

    /**
     * Checks whether the named property is indexed somewhere along the given
     * path. Lookup starts at the current path (at the root of this object) and
     * traverses down the path.
     * 
     * @param name property name
     * @param path lookup path
     * @return true if the property is indexed
     */
    public boolean isIndexed(String name, String path) {
        if (getIndexDefinitionNode(name) != null) {
            return true;
        }

        // TODO use PathUtils
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        int slash = path.indexOf('/');
        if (slash == -1) {
            return false;
        }

        NodeState child = root.getChildNode(path.substring(0, slash));
        return new Property2IndexLookup(child).isIndexed(
                name, path.substring(slash));
    }

    /**
     * Searches for a given <code>String<code> value within this index.
     * 
     * <p><b>Note</b> if the property you are looking for is not of type <code>String<code>, 
     * the converted key value might not match the index key, and there will be no hits on the index.</p>
     * 
     * @param name the property name
     * @param value the property value
     * @return the set of matched paths
     */
    public Set<String> find(String name, String value) {
        return find(name, PropertyValues.newString(value));
    }

    /**
     * Searches for a given value within this index.
     * 
     * @param name the property name
     * @param value the property value
     * @return the set of matched paths
     */
    public Set<String> find(String name, PropertyValue value) {
        Set<String> paths = Sets.newHashSet();

        NodeState state = getIndexDefinitionNode(name);
        if (state != null && state.getChildNode(":index") != null) {
            state = state.getChildNode(":index");
            paths.addAll(store.find(state, Property2Index.encode(value)));
        } else {
            // No index available, so first check this node for a match
            PropertyState property = root.getProperty(name);
            if (property != null) {
                if (value.isArray()) {
                    // let query engine handle multi-valued look ups
                    // simply return all nodes that have this property
                    paths.add("");
                } else {
                    // does it match any of the values of this property?
                    for (int i = 0; i < property.count(); i++) {
                        if (property.getValue(value.getType(), i).equals(value.getValue(value.getType()))) {
                            paths.add("");
                            // no need to check for more matches in this property
                            break;
                        }
                    }
                }
            }

            // ... and then recursively look up from the rest of the tree
            for (ChildNodeEntry entry : root.getChildNodeEntries()) {
                String base = entry.getName();
                Property2IndexLookup lookup =
                        new Property2IndexLookup(entry.getNodeState());
                for (String path : lookup.find(name, value)) {
                    if (path.isEmpty()) {
                        paths.add(base);
                    } else {
                        paths.add(base + "/" + path);
                    }
                }
            }
        }

        return paths;
    }

    public double getCost(String name, PropertyValue value) {
        double cost = 0.0;
        NodeState state = getIndexDefinitionNode(name);
        if (state != null && state.getChildNode(":index") != null) {
            state = state.getChildNode(":index");
            cost += store.count(state, Property2Index.encode(value));
        } else {
            cost = Double.POSITIVE_INFINITY;
        }
        return cost;
    }

    /**
     * Get the node with the index definition node for the given property.
     * 
     * @param name the property name
     * @return the node where the index definition is stored, or null if no
     *         index definition node was found
     */
    @Nullable
    private NodeState getIndexDefinitionNode(String name) {
        NodeState state = root.getChildNode(INDEX_DEFINITIONS_NAME);
        if (state != null) {
            for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                PropertyState type = entry.getNodeState().getProperty(IndexConstants.TYPE_PROPERTY_NAME);
                if(type == null || type.isArray() || !Property2Index.TYPE.equals(type.getValue(Type.STRING))){
                    continue;
                }
                PropertyState names = entry.getNodeState().getProperty("propertyNames");
                if (names != null) {
                    for (int i = 0; i < names.count(); i++) {
                        if (name.equals(names.getValue(Type.STRING, i))) {
                            return entry.getNodeState();
                        }
                    }
                }
            }
        }
        return null;
    }
}