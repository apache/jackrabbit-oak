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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Takes care of applying the updates to the index content.
 * <p>
 * The changes are temporarily added to an in-memory structure, and then applied
 * to the node.
 */
class Property2IndexUpdate {

    private final IndexStoreStrategy store;

    /**
     * The path of the index definition (where the index data is stored).
     */
    private final String path;

    /**
     * The node types that this index applies to. If <code>null</code> or
     * <code>empty</code> then the node type of the indexed node is ignored
     * 
     */
    private final List<String> nodeTypeNames;

    /**
     * The node where the index definition is stored.
     */
    private final NodeBuilder node;

    /**
     * The set of added values / paths. The key of the map is the property value
     * (encoded as a string), the value of the map is a set of paths that where
     * added.
     */
    private final Map<String, Set<String>> insert;

    /**
     * The set of removed values / paths. The key of the map is the property
     * value (encoded as a string), the value of the map is a set of paths that
     * were removed.
     */
    private final Map<String, Set<String>> remove;

    public Property2IndexUpdate(String path, NodeBuilder node,
            IndexStoreStrategy store) {
        this(path, node, store, null);
    }

    public Property2IndexUpdate(String path, NodeBuilder node,
            IndexStoreStrategy store, List<String> nodeTypeNames) {
        this.path = path;
        this.node = node;
        this.store = store;
        this.insert = Maps.newHashMap();
        this.remove = Maps.newHashMap();
        this.nodeTypeNames = nodeTypeNames;
    }

    String getPath() {
        return path;
    }

    public List<String> getNodeTypeNames() {
        return nodeTypeNames;
    }

    /**
     * A property value was added at the given path.
     * 
     * @param path the path
     * @param value the value
     */
    public void insert(String path, PropertyState value) {
        Preconditions.checkArgument(path.startsWith(this.path));
        putValues(insert, path.substring(this.path.length()), value);
    }

    /**
     * A property value was removed at the given path.
     * 
     * @param path the path
     * @param value the value
     */
    public void remove(String path, PropertyState value) {
        Preconditions.checkArgument(path.startsWith(this.path));
        putValues(remove, path.substring(this.path.length()), value);
    }

    private static void putValues(Map<String, Set<String>> map, String path,
            PropertyState value) {
        if (value.getType().tag() != PropertyType.BINARY) {
            List<String> keys = Property2Index.encode(PropertyValues
                    .create(value));
            for (String key : keys) {
                Set<String> paths = map.get(key);
                if (paths == null) {
                    paths = Sets.newHashSet();
                    map.put(key, paths);
                }
                if ("".equals(path)) {
                    path = "/";
                }
                paths.add(path);
            }
        }
    }

    boolean getAndResetReindexFlag() {
        boolean reindex = node.getProperty(REINDEX_PROPERTY_NAME) != null
                && node.getProperty(REINDEX_PROPERTY_NAME).getValue(
                        Type.BOOLEAN);
        node.setProperty(REINDEX_PROPERTY_NAME, false);
        return reindex;
    }

    /**
     * Try to apply the changes to the index content (to the ":index" node.
     * 
     * @throws CommitFailedException if a unique index was violated
     */
    public void apply() throws CommitFailedException {
        boolean unique = node.getProperty("unique") != null
                && node.getProperty("unique").getValue(Type.BOOLEAN);
        NodeBuilder index = node.child(":index");
        for (Map.Entry<String, Set<String>> entry : remove.entrySet()) {
            store.remove(index, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Set<String>> entry : insert.entrySet()) {
            store.insert(index, entry.getKey(), unique, entry.getValue());
        }
    }

}
