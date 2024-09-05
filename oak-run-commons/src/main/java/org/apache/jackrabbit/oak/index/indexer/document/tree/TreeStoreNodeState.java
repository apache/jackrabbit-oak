/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import static org.apache.jackrabbit.guava.common.collect.Iterators.transform;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.MemoryObject;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A node state of an Oak node that is stored in a tree store.
 *
 * This is mostly a wrapper. It allows iterating over the children and reading
 * children directly.
 */
public class TreeStoreNodeState implements NodeState, MemoryObject {

    private final NodeState delegate;
    private final String path;
    private final TreeStore treeStore;
    private final long estimatedMemory;

    public TreeStoreNodeState(NodeState delegate, String path, TreeStore treeStore, long estimatedMemory) {
        this.delegate = delegate;
        this.path = path;
        this.treeStore = treeStore;
        this.estimatedMemory = estimatedMemory;
    }

    @Override
    public long estimatedMemory() {
        return estimatedMemory;
    }

    @Override
    public boolean exists() {
        return delegate.exists();
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return delegate.hasProperty(name);
    }

    @Nullable
    @Override
    public PropertyState getProperty(@NotNull String name) {
        return delegate.getProperty(name);
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        return delegate.getBoolean(name);
    }

    @Override
    public long getLong(String name) {
        return delegate.getLong(name);
    }

    @Nullable
    @Override
    public String getString(String name) {
        return delegate.getString(name);
    }

    @NotNull
    @Override
    public Iterable<String> getStrings(@NotNull String name) {
        return delegate.getStrings(name);
    }

    @Nullable
    @Override
    public String getName(@NotNull String name) {
        return delegate.getName(name);
    }

    @NotNull
    @Override
    public Iterable<String> getNames(@NotNull String name) {
        return delegate.getNames(name);
    }

    @Override
    public long getPropertyCount() {
        return delegate.getPropertyCount();
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return delegate.getProperties();
    }

    @NotNull
    @Override
    public NodeBuilder builder() {
        return delegate.builder();
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        return AbstractNodeState.compareAgainstBaseState(this, base, diff);
    }

    // ~-------------------------------< child node access >

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return treeStore.getNodeState(PathUtils.concat(path, name)).exists();
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        return treeStore.getNodeState(PathUtils.concat(path, name));
    }

    @Override
    public long getChildNodeCount(long max) {
        long result = 0;
        Iterator<String> it = getChildNodeNamesIterator();
        while (it.hasNext()) {
            result++;
            if (result > max) {
                return Long.MAX_VALUE;
            }
            it.next();
        }
        return result;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return new Iterable<String>() {
            public Iterator<String> iterator() {
                return getChildNodeNamesIterator();
            }
        };
    }

    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return () -> transform(getChildNodeIterator(),
                s -> new MemoryChildNodeEntry(PathUtils.getName(s.getPath()), s.getNodeState()));
    }

    private Iterator<NodeStateEntry> getChildNodeIterator() {
        return transform(getChildNodeNamesIterator(),
                s -> treeStore.getNodeStateEntry(PathUtils.concat(path, s)));
    }

    Iterator<String> getChildNodeNamesIterator() {
        Iterator<Entry<String, String>> it = treeStore.getSession().iterator(path);
        return new Iterator<String>() {
            String current;
            {
                fetch();
            }

            private void fetch() {
                if (!it.hasNext()) {
                    current = null;
                } else {
                    Entry<String, String> e = it.next();
                    if (!e.getValue().isEmpty()) {
                        current = null;
                    } else {
                        String key = e.getKey();
                        int index = key.lastIndexOf('\t');
                        if (index < 0) {
                            throw new IllegalArgumentException(key);
                        }
                        current = key.substring(index + 1);
                    }
                }
            }

            public boolean hasNext() {
                return current != null;
            }

            public String next() {
                String result = current;
                if (result == null) {
                    throw new IllegalStateException();
                }
                fetch();
                return result;
            }
        };
    }

}