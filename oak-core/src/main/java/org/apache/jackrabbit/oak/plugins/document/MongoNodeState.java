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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * A {@link NodeState} implementation for the {@link MongoNodeStore}.
 * TODO: merge MongoNodeState with Node
 */
final class MongoNodeState extends AbstractNodeState {

    /**
     * The number of child nodes to fetch initially.
     */
    static final int INITIAL_FETCH_SIZE = 100;

    /**
     * The maximum number of child nodes to fetch in one call. (1600).
     */
    static final int MAX_FETCH_SIZE = INITIAL_FETCH_SIZE << 4;

    private final MongoNodeStore store;

    private final Node node;

    /**
     * TODO: OAK-1056
     */
    private boolean isBranch;

    MongoNodeState(@Nonnull MongoNodeStore store,
                   @Nonnull Node node) {
        this.store = checkNotNull(store);
        this.node = checkNotNull(node);
    }

    String getPath() {
        return node.getPath();
    }

    Revision getRevision() {
        return node.getReadRevision();
    }

    MongoNodeState setBranch() {
        isBranch = true;
        return this;
    }

    boolean isBranch() {
        return isBranch;
    }

    //--------------------------< NodeState >-----------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof MongoNodeState) {
            MongoNodeState other = (MongoNodeState) that;
            if (getPath().equals(other.getPath())) {
                return node.getLastRevision().equals(other.node.getLastRevision());
            }
        } else if (that instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) that;
            if (modified.getBaseState() == this) {
                return false;
            }
        }
        if (that instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) that);
        } else {
            return false;
        }
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public PropertyState getProperty(String name) {
        String value = node.getProperty(name);
        if (value == null) {
            return null;
        }
        return new MongoPropertyState(store, name, value);
    }

    @Override
    public boolean hasProperty(String name) {
        return node.getPropertyNames().contains(name);
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(node.getPropertyNames(), new Function<String, PropertyState>() {
            @Override
            public PropertyState apply(String name) {
                return getProperty(name);
            }
        });
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        if (node.hasNoChildren()) {
            return EmptyNodeState.MISSING_NODE;
        }
        String p = PathUtils.concat(getPath(), name);
        Node child = store.getNode(p, node.getLastRevision());
        if (child == null) {
            return EmptyNodeState.MISSING_NODE;
        } else {
            return new MongoNodeState(store, child);
        }
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (node.hasNoChildren()) {
            return Collections.emptyList();
        }
        return new Iterable<ChildNodeEntry>() {
            @Override
            public Iterator<ChildNodeEntry> iterator() {
                return new ChildNodeEntryIterator();
            }
        };
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        if (isBranch) {
            return new MemoryNodeBuilder(this);
        } else if ("/".equals(getPath())) {
            return new MongoRootBuilder(this, store);
        } else {
            return new MemoryNodeBuilder(this);
        }
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base) {
            return true;
        } else if (base == EMPTY_NODE || !base.exists()) { 
            // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (base instanceof MongoNodeState) {
            MongoNodeState mBase = (MongoNodeState) base;
            if (store == mBase.store) {
                if (node.getLastRevision().equals(mBase.node.getLastRevision())
                        && getPath().equals(mBase.getPath())) {
                    // no differences
                    return true; 
                }
                // TODO: use diff, similar to KernelNodeState
            }
        }
        // fall back to the generic node state diff algorithm
        return super.compareAgainstBaseState(base, diff);
    }

    //------------------------------< internal >--------------------------------

    /**
     * Returns up to {@code limit} child node entries, starting after the given
     * {@code name}.
     *
     * @param name the name of the lower bound child node entry (exclusive) or
     *             {@code null}, if the method should start with the first known
     *             child node.
     * @param limit the maximum number of child node entries to return.
     * @return the child node entries.
     */
    @Nonnull
    private Iterable<ChildNodeEntry> getChildNodeEntries(@Nullable String name,
                                                         int limit) {
        Iterable<Node> children = store.getChildNodes(node, name, limit);
        return Iterables.transform(children, new Function<Node, ChildNodeEntry>() {
            @Override
            public ChildNodeEntry apply(final Node input) {
                return new AbstractChildNodeEntry() {
                    @Nonnull
                    @Override
                    public String getName() {
                        return PathUtils.getName(input.path);
                    }

                    @Nonnull
                    @Override
                    public NodeState getNodeState() {
                        return new MongoNodeState(store, input);
                    }
                };
            }
        });
    }

    private class ChildNodeEntryIterator implements Iterator<ChildNodeEntry> {

        private String previousName;
        private Iterator<ChildNodeEntry> current;
        private int fetchSize = INITIAL_FETCH_SIZE;

        ChildNodeEntryIterator() {
            fetchMore();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (current == null) {
                    return false;
                } else if (current.hasNext()) {
                    return true;
                }
                fetchMore();
            }
        }

        @Override
        public ChildNodeEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ChildNodeEntry entry = current.next();
            previousName = entry.getName();
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void fetchMore() {
            Iterator<ChildNodeEntry> entries = getChildNodeEntries(
                    previousName, fetchSize).iterator();
            fetchSize = Math.min(fetchSize * 2, MAX_FETCH_SIZE);
            if (entries.hasNext()) {
                current = entries;
            } else {
                current = null;
            }
        }
    }
}
