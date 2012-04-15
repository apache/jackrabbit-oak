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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;
import org.apache.jackrabbit.oak.api.TransientNodeState;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.PagedIterator;
import org.apache.jackrabbit.oak.util.Predicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.jackrabbit.oak.util.Iterators.chain;
import static org.apache.jackrabbit.oak.util.Iterators.empty;
import static org.apache.jackrabbit.oak.util.Iterators.filter;
import static org.apache.jackrabbit.oak.util.Iterators.flatten;
import static org.apache.jackrabbit.oak.util.Iterators.map;

public class TransientKernelNodeState implements TransientNodeState {
    /** Branch this state belongs to */
    private final KernelBranch branch;

    /**
     * Underlying persistent state or {@code null} if this instance represents an
     * added node state
     */
    private final NodeState persistentState;

    /** Resolved persistent child states */
    private final Map<String, TransientKernelNodeState> existingChildNodes =
            new HashMap<String, TransientKernelNodeState>();

    /** Transiently added node states */
    private final Map<String, TransientKernelNodeState> addedNodes =
            new HashMap<String, TransientKernelNodeState>();

    /** Transiently removed node stated */
    private final Set<String> removedNodes = new HashSet<String>();

    /** Transiently added property states */
    private final Map<String, PropertyState> addedProperties =
            new HashMap<String, PropertyState>();

    /** Transiently removed property states */
    private final Set<String> removedProperties = new HashSet<String>();

    /** Name of this state */
    private String name;

    /** Parent of this state */
    private TransientNodeState parent;

    /**
     * Create a new instance representing the root of a branch.
     * @param branch  the branch this state belongs to
     * @param persistentState  underlying persistent state
     */
    TransientKernelNodeState(KernelBranch branch, NodeState persistentState) {
        this(branch, persistentState, null, "");
    }

    /**
     * Create a new instance representing a added node state
     * @param branch  the branch the state belongs to
     * @param parent  the parent state of the state
     * @param name  name of the state
     */
    private TransientKernelNodeState(KernelBranch branch, TransientNodeState parent, String name) {
        this(branch, null, parent, name);
    }

    /**
     * Create a new instance with an underlying persistent state
     * @param branch  the branch the state belongs to
     * @param persistedState  underlying persistent state
     * @param parent  the parent state of the state
     * @param name  name of the state
     */
    private TransientKernelNodeState(KernelBranch branch, NodeState persistedState,
            TransientNodeState parent, String name) {

        this.branch = branch;
        this.persistentState = persistedState;
        this.parent = parent;
        this.name = name;
    }

    /**
     * Copy constructor: create a deep copy of the passed {@code state} with
     * the given {@code name} and {@code parent}.
     * @param state  state to copy
     * @param parent  parent of the copied state
     * @param name  name of the copied state
     */
    private TransientKernelNodeState(TransientKernelNodeState state, TransientKernelNodeState parent,
            String name) {

        branch = state.branch;
        persistentState = state.persistentState;
        this.parent = parent;
        this.name = name;

        // recursively copy all existing node states
        for (Entry<String, TransientKernelNodeState> existing : state.existingChildNodes.entrySet()) {
            String existingName = existing.getKey();
            this.existingChildNodes.put(existingName,
                    new TransientKernelNodeState(existing.getValue(), this, existingName));
        }
        
        // recursively copy all added node states
        for (Entry<String, TransientKernelNodeState> added : state.addedNodes.entrySet()) {
            String addedName = added.getKey();
            this.addedNodes.put(addedName,
                    new TransientKernelNodeState(added.getValue(), this, addedName));
        }

        this.removedNodes.addAll(state.removedNodes);
        this.addedProperties.putAll(state.addedProperties);
        this.removedProperties.addAll(state.removedProperties);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPath() {
        if (parent == null) {
            return name;
        }
        else {
            String path = parent.getPath();
            return path.isEmpty()
                    ? name
                    : path + '/' + name;
        }
    }

    @Override
    public TransientNodeState getParent() {
        return parent;
    }

    @Override
    public KernelBranch getBranch() {
        return branch;
    }

    @Override
    public PropertyState getProperty(String name) {
        PropertyState state = addedProperties.get(name);
        if (state != null) {
            // Added or removed and re-added property
            return state;
        }

        // Existing property unless removed
        return removedProperties.contains(name) || persistentState == null
            ? null
            : persistentState.getProperty(name);
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        long persistentCount = persistentState == null
            ? 0
            : persistentState.getPropertyCount();
        
        return persistentCount + addedProperties.size() - removedProperties.size();
    }

    @Override
    public TransientKernelNodeState getChildNode(String name) {
        TransientKernelNodeState state = addedNodes.get(name);
        if (state != null) {
            // Added or removed and re-added child node
            return state;
        }

        // Existing child node unless removed
        return removedNodes.contains(name)
            ? null
            : getExistingChildNode(name);
    }

    @Override
    public boolean hasNode(String name) {
        return getChildNode(name) != null;
    }

    @Override
    public long getChildNodeCount() {
        long persistentCount = persistentState == null
                ? 0
                : persistentState.getChildNodeCount();

        return persistentCount + addedNodes.size() - removedNodes.size();
    }

    @Override
    public Iterable<PropertyState> getProperties() {
        // Persisted property states
        final Iterable<? extends PropertyState> persisted = persistentState == null
                ? null
                : persistentState.getProperties();

        // Copy of removed property states
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedProperties);

        // Copy of added and re-added property stated
        final Set<PropertyState> added = new HashSet<PropertyState>();
        added.addAll(addedProperties.values());

        // Filter removed property states from persisted property states
        // and add added property states
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                // persisted states
                Iterator<? extends PropertyState> properties =
                    persisted == null
                        ? Iterators.<PropertyState>empty()
                        : persisted.iterator();

                // persisted states - removed states
                Iterator<PropertyState> persistedMinusRemoved =
                        filter(properties, new Predicate<PropertyState>() {
                            @Override
                            public boolean evaluate(PropertyState state) {
                                return !removed.contains(state.getName());
                            }
                        });

                // persisted states - removed states + added states
                return chain(persistedMinusRemoved, added.iterator());
            }
        };
    }

    @Override
    public Iterable<TransientNodeState> getChildNodes() {
        // Copy od removed child node states
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedNodes);

        // Copy od added and re-added child node states
        final Set<TransientNodeState> added = new HashSet<TransientNodeState>();
        added.addAll(addedNodes.values());

        // Filter removed child node entries from persisted child node entries,
        // map remaining child node entries to child node states and add added
        // child node states
        return new Iterable<TransientNodeState>() {
            @Override
            public Iterator<TransientNodeState> iterator() {
                // persisted entries
                final Iterator<? extends ChildNodeEntry> persisted =
                    getPersistedChildNodeEntries(persistentState);

                // persisted entries - removed entries
                Iterator<ChildNodeEntry> persistedMinusRemovedEntries =
                    filter(persisted, new Predicate<ChildNodeEntry>() {
                        @Override
                        public boolean evaluate(ChildNodeEntry entry) {
                            return !removed.contains(entry.getName());
                        }
                    });

                // persisted states - removed states
                Iterator<TransientNodeState> persistedMinusRemoved =
                    map(persistedMinusRemovedEntries,
                        new Function1<ChildNodeEntry, TransientNodeState>() {
                            @Override
                            public TransientNodeState apply(ChildNodeEntry entry) {
                                return getExistingChildNode(entry.getName());
                            }
                        });

                // persisted states - removed states + added states
                return chain(persistedMinusRemoved, added.iterator());
            }
        };
    }

    @Override
    public TransientNodeState addNode(String name) {
        if (!hasNode(name)) {
            addedNodes.put(name, new TransientKernelNodeState(branch, this, name));
            branch.addNode(this, name);
        }

        return getChildNode(name);
    }

    @Override
    public void removeNode(String name) {
        if (hasNode(name)) {
            markNodeRemoved(name);
            branch.removeNode(this, name);
        }
    }

    @Override
    public void setProperty(String name, Scalar value) {
        PropertyState propertyState = new KernelPropertyState(name, value);
        setProperty(propertyState);
        branch.setProperty(this, name, value);
    }

    @Override
    public void setProperty(String name, List<Scalar> values) {
        PropertyState propertyState = new KernelPropertyState(name, values);
        setProperty(propertyState);
        branch.setProperty(this, name, values);
    }

    @Override
    public void removeProperty(String name) {
        addedProperties.remove(name);
        if (hasExistingProperty(name)) {
            // Mark as removed if removing existing
            removedProperties.add(name);
        }
        branch.removeProperty(this, name);
    }

//------------------------------------------------------------< internal >---

    void markNodeRemoved(String name) {
        addedNodes.remove(name);
        if (hasExistingNode(name)) {
            // Mark as removed if removing existing
            removedNodes.add(name);
        }
    }

    void setProperty(PropertyState state) {
        if (hasExistingProperty(state.getName())) {
            removedProperties.add(state.getName());
        }
        addedProperties.put(state.getName(), state);
    }

    void move(String name, TransientKernelNodeState destParent, String destName) {
        TransientKernelNodeState state = getChildNode(name);
        markNodeRemoved(name);

        state.name = destName;
        state.parent = destParent;
        destParent.addedNodes.put(destName, state);
    }

    void copy(String name, TransientKernelNodeState destParent, String destName) {
        destParent.addedNodes.put(destName,
                new TransientKernelNodeState(getChildNode(name), destParent, destName));
    }

    /**
     * Get a transient node state for a child node state which has
     * an existing underlying persistent node date.
     *
     * @param name  name of the child node state
     * @return  transient node state or {@code null} if this transient
     *          node state does not have an underlying persistent state
     *          or the underlying persistent state does not have a child
     *          node state with the given {@code name}.
     */
    private TransientKernelNodeState getExistingChildNode(String name) {
        if (persistentState == null) {
            return null;
        }

        TransientKernelNodeState transientState = existingChildNodes.get(name);
        if (transientState == null) {
            NodeState state = persistentState.getChildNode(name);
            if (state == null) {
                return null;
            }
            transientState = new TransientKernelNodeState(branch, state, this, name);
            existingChildNodes.put(name, transientState);
        }
        return transientState;
    }

    /**
     * Determine whether there is an underling persistent state which has
     * a child node state with the given {@code name}.
     * @param name  name of the child node state.
     * @return  {@code true} if and only if this transient node state has an
     *          underlying persistent state which has a child node state with
     *          the given {@code name}.
     */
    private boolean hasExistingNode(String name) {
        return persistentState != null && persistentState.getChildNode(name) != null;
    }

    /**
     * Determine whether there is an underling persistent state which has
     * a property state with the given {@code name}.
     * @param name  name of the property state.
     * @return  {@code true} if and only if this transient node state has an
     *          underlying persistent state which has a property state with
     *          the given {@code name}.
     */
    private boolean hasExistingProperty(String name) {
        return persistentState != null && persistentState.getProperty(name) != null;
    }

    /**
     * Iterator over all persisted child node entries of the given
     * {@code persistentState}. This iterator reads the child node entries page wise
     * with a page size of 1024 items.
     * @param persistentState  persistent state for retrieving the child node entries from
     * @return  iterator of child node entries
     */
    private static Iterator<? extends ChildNodeEntry> getPersistedChildNodeEntries(
            final NodeState persistentState) {

        if (persistentState == null) {
            return empty();
        }
        else {
            return flatten(
                new PagedIterator<ChildNodeEntry>(1024) {
                    @Override
                    protected Iterator<? extends ChildNodeEntry> getPage(long pos, int size) {
                        return persistentState.getChildNodeEntries(pos, size).iterator();
                    }
                });
        }
    }

}
