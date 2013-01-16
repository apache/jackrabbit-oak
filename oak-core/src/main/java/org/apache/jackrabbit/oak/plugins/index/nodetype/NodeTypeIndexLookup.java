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
package org.apache.jackrabbit.oak.plugins.index.nodetype;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexLookup;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Iterables;

/**
 * <code>NodeTypeIndexLookup</code> uses {@link PropertyIndexLookup} internally
 * for cost calculation and queries.
 */
class NodeTypeIndexLookup implements JcrConstants {

    private final NodeState root;

    public NodeTypeIndexLookup(NodeState root) {
        this.root = root;
    }

    /**
     * Returns <code>true</code> if a node type index lookup exists at the given
     * <code>path</code> or further up the tree.
     *
     * @param path the path to check.
     * @return <code>true</code> if a node type index exists; <code>false</code>
     *         otherwise.
     */
    public boolean isIndexed(String path) {
        Property2IndexLookup lookup = new Property2IndexLookup(root);
        if (lookup.isIndexed(JCR_PRIMARYTYPE, path)
                && lookup.isIndexed(JCR_MIXINTYPES, path)) {
            return true;
        }

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        int slash = path.indexOf('/');
        if (slash == -1) {
            return false;
        }

        NodeState child = root.getChildNode(path.substring(0, slash));
        return new NodeTypeIndexLookup(child).isIndexed(
                path.substring(slash));
    }

    public double getCost(Iterable<String> nodeTypes) {
        PropertyValue ntNames = PropertyValues.newName(nodeTypes);
        Property2IndexLookup lookup = new Property2IndexLookup(root);
        return lookup.getCost(JCR_PRIMARYTYPE, ntNames)
                + lookup.getCost(JCR_MIXINTYPES, ntNames);
    }
    
    /**
     * Returns the paths that match the given node types.
     *
     * @param filter the filter (used for logging)
     * @param nodeTypes the names of the node types to match.
     * @return the matched paths (the result might contain duplicate entries)
     */
    public Iterable<String> query(Filter filter, Iterable<String> nodeTypes) {
        final PropertyValue ntNames = PropertyValues.newName(nodeTypes);
        Property2IndexLookup lookup = new Property2IndexLookup(root);
        return Iterables.concat(
                lookup.query(filter, JCR_PRIMARYTYPE, ntNames),
                lookup.query(filter, JCR_MIXINTYPES, ntNames));
    }

}
