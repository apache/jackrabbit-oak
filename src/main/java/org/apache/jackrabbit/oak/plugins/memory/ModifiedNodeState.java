/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeState;

public class ModifiedNodeState extends ProxyNodeState {

    private final Map<String, PropertyState> properties;

    private final Map<String, NodeState> nodes;

    public ModifiedNodeState(
            NodeState base,
            Map<String, PropertyState> properties,
            Map<String, NodeState> nodes) {
        super(base);
        this.properties = properties;
        this.nodes = nodes;
    }

    NodeState getBase() {
        return delegate;
    }

    ModifiedNodeState rebase(MemoryNodeStore store, NodeState base)
            throws CommitFailedException {
        if (delegate.equals(base)) {
            return this;
        } else if (nodes.isEmpty()) {
            return this; // shortcut
        } else {
            return new ModifiedNodeState(
                    base, properties, rebaseChildren(store, base));
        }
    }

    private Map<String, NodeState> rebaseChildren(
            final MemoryNodeStore store, NodeState base)
            throws CommitFailedException {
        // TODO: better conflict resolution
        final Map<String, NodeState> rebasedNodes =
                new HashMap<String, NodeState>(nodes);
        final Map<String, CommitFailedException> failures =
                new HashMap<String, CommitFailedException>();
        store.compare(delegate, base, new DefaultNodeStateDiff() {
            @Override
            public void childNodeAdded(String name, NodeState after) {
                rebaseChild(name, after);
            }
            @Override
            public void childNodeChanged(
                    String name, NodeState before, NodeState after) {
                rebaseChild(name, after);
            }
            @Override
            public void childNodeDeleted(String name, NodeState before) {
                rebaseChild(name, MemoryNodeState.EMPTY_NODE);
            }
            private void rebaseChild(String name, NodeState base) {
                NodeState child = nodes.get(name);
                if (child != null) {
                    try {
                        rebasedNodes.put(name, store.rebase(child, base));
                    } catch (CommitFailedException e) {
                        failures.put(name, e);
                    }
                }
            }
        });
        if (failures.isEmpty()) {
            return rebasedNodes;
        } else {
            throw new CommitFailedException("Failed to rebase changes");
        }
    }

    //---------------------------------------------------------< NodeState >--

    @Override
    public PropertyState getProperty(String name) {
        if (properties.containsKey(name)) {
            return properties.get(name);
        } else {
            return super.getProperty(name);
        }
    }

    @Override
    public long getPropertyCount() {
        long count = super.getPropertyCount();
        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
            if (super.getProperty(entry.getKey()) != null) {
                if (entry.getValue() == null) {
                    count--;
                }
            } else {
                if (entry.getValue() != null) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        if (properties.isEmpty()) {
            return super.getProperties(); // shortcut
        }
        final Iterable<? extends PropertyState> unmodified =
                super.getProperties();
        final Iterable<? extends PropertyState> modified =
                properties.values();
        return new Iterable<PropertyState>() {
            @Override @SuppressWarnings("unchecked")
            public Iterator<PropertyState> iterator() {
                Iterator<PropertyState> a = IteratorUtils.filteredIterator(
                        unmodified.iterator(),
                        new UnmodifiedPropertyPredicate());
                Iterator<PropertyState> b = IteratorUtils.filteredIterator(
                        modified.iterator(),
                        PredicateUtils.notNullPredicate());
                return IteratorUtils.chainedIterator(a, b);
            }
        };
    }

    @Override
    public NodeState getChildNode(String name) {
        if (nodes.containsKey(name)) {
            return nodes.get(name);
        } else {
            return super.getChildNode(name);
        }
    }

    @Override
    public long getChildNodeCount() {
        long count = super.getPropertyCount();
        for (Map.Entry<String, NodeState> entry : nodes.entrySet()) {
            if (super.getChildNode(entry.getKey()) != null) {
                if (entry.getValue() == null) {
                    count--;
                }
            } else {
                if (entry.getValue() != null) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (nodes.isEmpty()) {
            return super.getChildNodeEntries(); // shortcut
        }
        final Iterable<? extends ChildNodeEntry> unmodified =
                super.getChildNodeEntries();
        final Iterable<? extends ChildNodeEntry> modified =
                MemoryChildNodeEntry.iterable(nodes);
        return new Iterable<ChildNodeEntry>() {
            @Override @SuppressWarnings("unchecked")
            public Iterator<ChildNodeEntry> iterator() {
                Iterator<ChildNodeEntry> a = IteratorUtils.filteredIterator(
                        unmodified.iterator(),
                        new UnmodifiedChildNodePredicate());
                Iterator<ChildNodeEntry> b = IteratorUtils.filteredIterator(
                        modified.iterator(),
                        new UndeletedChildNodePredicate());
                return IteratorUtils.chainedIterator(a, b);
            }
        };
    }

    private class UnmodifiedPropertyPredicate implements Predicate {

        @Override
        public boolean evaluate(Object object) {
            if (object instanceof PropertyState) {
                PropertyState property = ((PropertyState) object);
                return !properties.containsKey(property.getName());
            } else {
                return false;
            }
        }

    }

    private class UnmodifiedChildNodePredicate implements Predicate {

        @Override
        public boolean evaluate(Object object) {
            if (object instanceof ChildNodeEntry) {
                ChildNodeEntry entry = ((ChildNodeEntry) object);
                return !nodes.containsKey(entry.getName());
            } else {
                return false;
            }
        }

    }

    private class UndeletedChildNodePredicate implements Predicate {

        @Override
        public boolean evaluate(Object object) {
            if (object instanceof ChildNodeEntry) {
                ChildNodeEntry entry = ((ChildNodeEntry) object);
                return entry.getNodeState() != null;
            } else {
                return false;
            }
        }

    }

}
