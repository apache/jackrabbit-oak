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

import org.apache.jackrabbit.oak.api.ContentTree;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;
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

public class KernelContentTree implements ContentTree {

    /**
     * Underlying persistent state or {@code null} if this instance represents an
     * added content tree
     */
    private final NodeState persistentState;

    /** Parent of this content tree */
    private KernelContentTree parent;

    /** Name of this content tree */
    private String name;

    /** Listener for changes on this content tree */
    private final Listener listener;

    /** Children with underlying persistent child states */
    private final Map<String, KernelContentTree> existingChildren =
            new HashMap<String, KernelContentTree>();

    /** Transiently added children */
    private final Map<String, KernelContentTree> addedTrees =
            new HashMap<String, KernelContentTree>();

    /** Transiently removed children */
    private final Set<String> removedTrees = new HashSet<String>();

    /** Transiently added property states */
    private final Map<String, PropertyState> addedProperties =
            new HashMap<String, PropertyState>();

    /** Transiently removed property states */
    private final Set<String> removedProperties = new HashSet<String>();

    /**
     * Listener for changes on {@code ContentTree}s
     */
    interface Listener {

        /**
         * The child of the given {@code name} has been added to {@code tree}.
         * @param tree  parent to which a child was added
         * @param name  name of the added child
         */
        void addChild(KernelContentTree tree, String name);

        /**
         * The child of the given {@code name} has been removed from {@code tree}
         * @param tree  parent from which a child was removed
         * @param name  name of the removed child
         */
        void removeChild(KernelContentTree tree, String name);

        /**
         * The property of the given {@code name} and {@code value} has been set.
         * @param tree  parent on which the property was set.
         * @param name  name of the property
         * @param value  value of the property
         */
        void setProperty(KernelContentTree tree, String name, Scalar value);

        /**
         * The property of the given {@code name} and {@code values} has been set.
         * @param tree  parent on which the property was set.
         * @param name  name of the property
         * @param values  values of the property
         */
        void setProperty(KernelContentTree tree, String name, List<Scalar> values);

        /**
         * The property of the given {@code name} has been removed.
         * @param tree  parent on which the property was removed.
         * @param name  name of the property
         */
        void removeProperty(KernelContentTree tree, String name);

        /**
         * The child with the given {@code name} has been moved.
         * @param tree  parent from which the child was moved
         * @param name  name of the moved child
         * @param moved  moved child
         */
        void move(KernelContentTree tree, String name, KernelContentTree moved);

        /**
         * The child with the given {@code name} been copied.
         * @param state  parent from which the child way copied
         * @param name  name of the copied child
         * @param copied  copied child
         */
        void copy(KernelContentTree state, String name, KernelContentTree copied);
    }

    /**
     * Create a new instance representing the root of a branch.
     * @param persistentState  underlying persistent state
     * @param listener  change listener
     */
    KernelContentTree(NodeState persistentState, Listener listener) {
        this(persistentState, null, "", listener);
    }

    /**
     * Create a new instance representing an added child
     * @param parent  the parent of the child
     * @param name  name of the child
     * @param listener  change listener
     */
    private KernelContentTree(KernelContentTree parent, String name, Listener listener) {
        this(null, parent, name, listener);
    }

    /**
     * Create a new instance with an underlying persistent state
     * @param persistedState  underlying persistent state
     * @param parent  the parent of this content tree
     * @param name  name of this content tree
     * @param listener  change listener
     */
    private KernelContentTree(NodeState persistedState, KernelContentTree parent,
            String name, Listener listener) {

        this.persistentState = persistedState;
        this.parent = parent;
        this.name = name;
        this.listener = listener;
    }

    /**
     * Copy constructor: create a deep copy of the passed {@code ContentTree} with
     * the given {@code name} and {@code parent}.
     * @param tree  content tree to copy
     * @param parent  parent of the copied tree
     * @param name  name of the copied tree
     */
    private KernelContentTree(KernelContentTree tree, KernelContentTree parent,
            String name) {

        listener = tree.listener;
        persistentState = tree.persistentState;
        this.parent = parent;
        this.name = name;

        // recursively copy all existing children
        for (Entry<String, KernelContentTree> existing : tree.existingChildren.entrySet()) {
            String existingName = existing.getKey();
            this.existingChildren.put(existingName,
                    new KernelContentTree(existing.getValue(), this, existingName));
        }
        
        // recursively copy all added children
        for (Entry<String, KernelContentTree> added : tree.addedTrees.entrySet()) {
            String addedName = added.getKey();
            this.addedTrees.put(addedName,
                    new KernelContentTree(added.getValue(), this, addedName));
        }

        this.removedTrees.addAll(tree.removedTrees);
        this.addedProperties.putAll(tree.addedProperties);
        this.removedProperties.addAll(tree.removedProperties);
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
    public ContentTree getParent() {
        return parent;
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
    public KernelContentTree getChild(String name) {
        KernelContentTree state = addedTrees.get(name);
        if (state != null) {
            // Added or removed and re-added child
            return state;
        }

        // Existing child unless removed
        return removedTrees.contains(name)
            ? null
            : getExistingChild(name);
    }

    @Override
    public boolean hasChild(String name) {
        return getChild(name) != null;
    }

    @Override
    public long getChildrenCount() {
        long persistentCount = persistentState == null
                ? 0
                : persistentState.getChildNodeCount();

        return persistentCount + addedTrees.size() - removedTrees.size();
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
    public Iterable<ContentTree> getChildren() {
        // Copy of removed children
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedTrees);

        // Copy od added and re-added children
        final Set<ContentTree> added = new HashSet<ContentTree>();
        added.addAll(addedTrees.values());

        // Filter removed child node entries from persisted child node entries,
        // map remaining child node entries to content trees and add added children.
        return new Iterable<ContentTree>() {
            @Override
            public Iterator<ContentTree> iterator() {
                // persisted entries
                final Iterator<? extends ChildNodeEntry> persisted =
                    getPersistedChildren(persistentState);

                // persisted entries - removed entries
                Iterator<ChildNodeEntry> persistedMinusRemovedEntries =
                    filter(persisted, new Predicate<ChildNodeEntry>() {
                        @Override
                        public boolean evaluate(ChildNodeEntry entry) {
                            return !removed.contains(entry.getName());
                        }
                    });

                // persisted trees - removed trees
                Iterator<ContentTree> persistedMinusRemoved =
                    map(persistedMinusRemovedEntries,
                        new Function1<ChildNodeEntry, ContentTree>() {
                            @Override
                            public ContentTree apply(ChildNodeEntry entry) {
                                return getExistingChild(entry.getName());
                            }
                        });

                // persisted trees - removed trees + added trees
                return chain(persistedMinusRemoved, added.iterator());
            }
        };
    }

    @Override
    public ContentTree addChild(String name) {
        if (!hasChild(name)) {
            addedTrees.put(name, new KernelContentTree(this, name, listener));
            if (listener != null) {
                listener.addChild(this, name);
            }
        }

        return getChild(name);
    }

    @Override
    public boolean removeChild(String name) {
        if (hasChild(name)) {
            markTreeRemoved(name);
            if (listener != null) {
                listener.removeChild(this, name);
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void setProperty(String name, Scalar value) {
        PropertyState propertyState = new KernelPropertyState(name, value);
        setProperty(propertyState);
        if (listener != null) {
            listener.setProperty(this, name, value);
        }
    }

    @Override
    public void setProperty(String name, List<Scalar> values) {
        PropertyState propertyState = new KernelPropertyState(name, values);
        setProperty(propertyState);
        if (listener != null) {
            listener.setProperty(this, name, values);
        }
    }

    @Override
    public void removeProperty(String name) {
        addedProperties.remove(name);
        if (hasExistingProperty(name)) {
            // Mark as removed if removing existing
            removedProperties.add(name);
        }
        if (listener != null) {
            listener.removeProperty(this, name);
        }
    }

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     *
     * @param destParent  new parent for this tree
     * @param destName  new name for this tree
     */
    public void move(KernelContentTree destParent, String destName) {
        parent.markTreeRemoved(name);

        KernelContentTree oldParent = parent;
        String oldName = name;

        name = destName;
        parent = destParent;
        destParent.addedTrees.put(destName, this);
        if (listener != null) {
            listener.move(oldParent, oldName, this);
        }
    }

    /**
     * Copy this tree to the parent at {@code destParent} with the name {@code destName}.
     *
     * @param destParent  parent for the copied tree
     * @param destName  name for the copied tree
     */
    public void copy(KernelContentTree destParent, String destName) {
        KernelContentTree copy = new KernelContentTree(this, destParent, destName);
        destParent.addedTrees.put(destName, copy);
        if (listener != null) {
            listener.copy(parent, name, copy);
        }
    }

    //------------------------------------------------------------< internal >---

    private void markTreeRemoved(String name) {
        addedTrees.remove(name);
        if (hasExistingChild(name)) {
            // Mark as removed if removing existing
            removedTrees.add(name);
        }
    }

    private void setProperty(PropertyState state) {
        if (hasExistingProperty(state.getName())) {
            removedProperties.add(state.getName());
        }
        addedProperties.put(state.getName(), state);
    }

    /**
     * Get a content tree for a child which has an existing underlying persistent
     * node date.
     *
     * @param name  name of the child
     * @return  content tree or {@code null} if this instance node state
     *          does not have an underlying persistent state or the underlying
     *          persistent state does not have a child with the given {@code name}.
     */
    private KernelContentTree getExistingChild(String name) {
        if (persistentState == null) {
            return null;
        }

        KernelContentTree transientState = existingChildren.get(name);
        if (transientState == null) {
            NodeState state = persistentState.getChildNode(name);
            if (state == null) {
                return null;
            }
            transientState = new KernelContentTree(state, this, name, listener);
            existingChildren.put(name, transientState);
        }
        return transientState;
    }

    /**
     * Determine whether there is an underling persistent state which has
     * a child with the given {@code name}.
     * @param name  name of the child.
     * @return  {@code true} if and only if this instance has an underlying persistent
     *          state which has a child with the given {@code name}.
     */
    private boolean hasExistingChild(String name) {
        return persistentState != null && persistentState.getChildNode(name) != null;
    }

    /**
     * Determine whether there is an underling persistent state which has
     * a property state with the given {@code name}.
     * @param name  name of the property state.
     * @return  {@code true} if and only if this instance has an underlying persistent
     *          state which has a property state with the given {@code name}.
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
    private static Iterator<? extends ChildNodeEntry> getPersistedChildren(
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
