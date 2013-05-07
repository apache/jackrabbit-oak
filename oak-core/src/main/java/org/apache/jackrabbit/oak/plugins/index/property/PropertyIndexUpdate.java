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

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getBoolean;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.encode;

import java.util.Collections;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Takes care of applying the updates to the index content.
 */
class PropertyIndexUpdate {

    private final IndexStoreStrategy store;

    /**
     * The path of the index definition (where the index data is stored).
     */
    private final String path;

    /**
     * Primary node types that this index applies to. If <code>null</code>
     * then the node type of the indexed node is ignored
     */
    private final Set<String> primaryTypes;

    /**
     * Mixin node types that this index applies to. If <code>null</code>
     * then the node type of the indexed node is ignored
     */
    private final Set<String> mixinTypes;

    /**
     * The node where the index definition is stored.
     */
    private final NodeBuilder node;

    /**
     * The node where the index content is stored.
     */
    private final NodeBuilder index;

    private final boolean unique;

    private final Set<String> modifiedKeys = Sets.newHashSet();

    public PropertyIndexUpdate(
            String path, NodeBuilder node, IndexStoreStrategy store,
            Set<String> primaryTypes, Set<String> mixinTypes) {
        this.path = path;
        this.node = node;
        this.store = store;

        if (primaryTypes.isEmpty() && mixinTypes.isEmpty()) {
            this.primaryTypes = null;
            this.mixinTypes = null;
        } else {
            this.primaryTypes = primaryTypes;
            this.mixinTypes = mixinTypes;
        }

        index = this.node.child(INDEX_CONTENT_NODE_NAME);
        unique = getBoolean(node, UNIQUE_PROPERTY_NAME);
    }

    String getPath() {
        return path;
    }

    /**
     * A property value was added at the given path.
     * 
     * @param path
     *            the path
     * @param value
     *            the value
     */
    void insert(String path, PropertyState value) throws CommitFailedException {
        Preconditions.checkArgument(path.startsWith(this.path));
        if (value.getType().tag() == PropertyType.BINARY) {
            return;
        }
        for (String key : encode(PropertyValues.create(value))) {
            store.insert(index, key, ImmutableSet.of(trimm(path)));
            modifiedKeys.add(key);
        }
    }

    /**
     * A property value was removed at the given path.
     * 
     * @param path
     *            the path
     * @param value
     *            the value
     */
    public void remove(String path, PropertyState value)
            throws CommitFailedException {
        Preconditions.checkArgument(path.startsWith(this.path));
        if (value.getType().tag() == PropertyType.BINARY) {
            return;
        }
        for (String key : encode(PropertyValues.create(value))) {
            store.remove(index, key, ImmutableSet.of(trimm(path)));
            modifiedKeys.add(key);
        }
    }

    public void checkUniqueKeys() throws CommitFailedException {
        if (unique && !modifiedKeys.isEmpty()) {
            NodeState state = index.getNodeState();
            for (String key : modifiedKeys) {
                if (store.count(state, Collections.singletonList(key), 2) > 1) {
                    throw new CommitFailedException(
                            "Constraint", 30,
                            "Uniqueness constraint violated for key " + key);
                }
            }
        }
    }

    private String trimm(String path) {
        path = path.substring(this.path.length());
        if ("".equals(path)) {
            return "/";
        }
        return path;
    }

    public boolean matches(
            String path, Set<String> primaryTypes, Set<String> mixinTypes) {
        if (this.primaryTypes == null) {
            return this.path.equals(path)
                    && primaryTypes.isEmpty()
                    && mixinTypes.isEmpty();
        } else {
            return this.path.equals(path)
                    && this.primaryTypes.equals(primaryTypes)
                    && this.mixinTypes.equals(mixinTypes);
        }
    }

    public boolean matchesNodeType(NodeBuilder node, String path) {
        if (!path.startsWith(this.path)) {
            return false;
        }
        if (primaryTypes == null
                || primaryTypes.contains(node.getName(JCR_PRIMARYTYPE))) {
            return true;
        }
        if (!mixinTypes.isEmpty()) {
            for (String mixinName : node.getNames(JCR_MIXINTYPES)) {
                if (mixinTypes.contains(mixinName)) {
                    return true;
                }
            }
        }
        return false;
    }

}
