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

import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
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
        return isIndexed(root, name, path);
    }
    
    private static boolean isIndexed(NodeState root, String name, String path) {
        NodeState node = root;
        Iterator<String> it = PathUtils.elements(path).iterator();
        while (true) {
            if (getIndexDataNode(node, name) != null) {
                return true;
            }
            if (!it.hasNext()) {
                break;
            }
            node = node.getChildNode(it.next());
        }
        return false;
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
     * @param value the property value (null to check for property existence)
     * @return the set of matched paths
     */
    public Set<String> find(String name, PropertyValue value) {
        NodeState state = getIndexDataNode(root, name);
        if (state == null) {
            throw new IllegalArgumentException("No index for " + name);
        }
        Set<String> paths = Sets.newHashSet();
        if (value == null) {
            paths.addAll(store.find(state, null));
        } else {
            paths.addAll(store.find(state, Property2Index.encode(value)));
        }
        return paths;
    }

    public double getCost(String name, PropertyValue value) {
        // TODO the cost method is currently reading all the data - 
        // is not supposed to do that, it is only supposed to estimate
        NodeState state = getIndexDataNode(root, name);
        if (state == null) {
            return Double.POSITIVE_INFINITY;
        }
        double cost;
        if (value == null) {
            cost = store.count(state, null);
        } else {
            cost = store.count(state, Property2Index.encode(value));
        }
        return cost;
    }

    /**
     * Get the node with the index data for the given property, if there is an
     * applicable index with data.
     * 
     * @param name the property name
     * @return the node where the index data is stored, or null if no index
     *         definition or index data node was found
     */
    @Nullable
    private static NodeState getIndexDataNode(NodeState node, String name) {
        NodeState state = node.getChildNode(INDEX_DEFINITIONS_NAME);
        if (state != null) {
            for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                PropertyState type = entry.getNodeState().getProperty(IndexConstants.TYPE_PROPERTY_NAME);
                if (type == null || type.isArray() || !Property2Index.TYPE.equals(type.getValue(Type.STRING))) {
                    continue;
                }
                PropertyState names = entry.getNodeState().getProperty("propertyNames");
                if (names != null) {
                    for (int i = 0; i < names.count(); i++) {
                        if (name.equals(names.getValue(Type.STRING, i))) {
                            NodeState indexDef = entry.getNodeState();
                            NodeState index = indexDef.getChildNode(":index");
                            if (index != null) {
                                return index;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
}