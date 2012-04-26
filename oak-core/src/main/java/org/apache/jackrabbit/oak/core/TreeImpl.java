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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.kernel.ChildNodeEntry;
import org.apache.jackrabbit.oak.kernel.NodeState;
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

/**
 * TODO: Refactor to be based on the individual NodeStateBuilders instead of NodeStates
 */
public class TreeImpl implements Tree {

    /**
     * Underlying persistent state or {@code null} if this instance represents an
     * added content tree
     */
    private final NodeState persistentState;

    /** Parent of this content tree */
    private TreeImpl parent;

    /** Name of this content tree */
    private String name;

    /** Listener for changes on this content tree */
    private final Listener listener;

    /** Children with underlying persistent child states */
    private final Map<String, TreeImpl> existingChildren =
            new HashMap<String, TreeImpl>();

    /** Transiently added children */
    private final Map<String, TreeImpl> addedTrees =
            new HashMap<String, TreeImpl>();

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
        void addChild(TreeImpl tree, String name);

        /**
         * The child of the given {@code name} has been removed from {@code tree}
         * @param tree  parent from which a child was removed
         * @param name  name of the removed child
         */
        void removeChild(TreeImpl tree, String name);

        /**
         * The property of the given {@code name} and {@code value} has been set.
         * @param tree  parent on which the property was set.
         * @param name  name of the property
         * @param value  value of the property
         */
        void setProperty(TreeImpl tree, String name, CoreValue value);

        /**
         * The property of the given {@code name} and {@code values} has been set.
         * @param tree  parent on which the property was set.
         * @param name  name of the property
         * @param values  values of the property
         */
        void setProperty(TreeImpl tree, String name, List<CoreValue> values);

        /**
         * The property of the given {@code name} has been removed.
         * @param tree  parent on which the property was removed.
         * @param name  name of the property
         */
        void removeProperty(TreeImpl tree, String name);

        /**
         * The child with the given {@code name} has been moved.
         * @param tree  parent from which the child was moved
         * @param name  name of the moved child
         * @param moved  moved child
         */
        void move(TreeImpl tree, String name, TreeImpl moved);

        /**
         * The child with the given {@code name} been copied.
         * @param tree  parent from which the child way copied
         * @param name  name of the copied child
         * @param copied  copied child
         */
        void copy(TreeImpl tree, String name, TreeImpl copied);
    }

    /**
     * Create a new instance representing the root of a tree
     * @param persistentState  underlying persistent state
     * @param listener  change listener
     */
    TreeImpl(NodeState persistentState, Listener listener) {
        this(persistentState, null, "", listener);
    }

    /**
     * Create a new instance representing an added child
     * @param parent  the parent of the child
     * @param name  name of the child
     * @param listener  change listener
     */
    private TreeImpl(TreeImpl parent, String name, Listener listener) {
        this(null, parent, name, listener);
    }

    /**
     * Create a new instance with an underlying persistent state
     * @param persistedState  underlying persistent state
     * @param parent  the parent of this content tree
     * @param name  name of this content tree
     * @param listener  change listener
     */
    private TreeImpl(NodeState persistedState, TreeImpl parent,
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
    private TreeImpl(TreeImpl tree, TreeImpl parent,
                     String name) {

        listener = tree.listener;
        persistentState = tree.persistentState;
        this.parent = parent;
        this.name = name;

        // recursively copy all existing children
        for (Entry<String, TreeImpl> existing : tree.existingChildren.entrySet()) {
            String existingName = existing.getKey();
            this.existingChildren.put(existingName,
                    new TreeImpl(existing.getValue(), this, existingName));
        }
        
        // recursively copy all added children
        for (Entry<String, TreeImpl> added : tree.addedTrees.entrySet()) {
            String addedName = added.getKey();
            this.addedTrees.put(addedName,
                    new TreeImpl(added.getValue(), this, addedName));
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
    public Tree getParent() {
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
    public Status getPropertyStatus(String name) {
        if (addedProperties.containsKey(name)) {
            if (persistentState.getProperty(name) == null) {
                return Status.NEW;
            }
            else {
                return Status.MODIFIED;
            }
        }
        else if (removedProperties.contains(name)) {
            return Status.REMOVED;
        }
        else if (persistentState.getProperty(name) == null) {
            return null;
        }
        else {
            return Status.EXISTING;
        }
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
    public TreeImpl getChild(String name) {
        TreeImpl state = addedTrees.get(name);
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
    public Status getChildStatus(String name) {
        if (addedTrees.containsKey(name)) {
            return Status.NEW;
        }
        else if (removedTrees.contains(name)) {
            return Status.REMOVED;
        }
        else {
            TreeImpl child = getChild(name);
            if (child == null) {
                return null;
            }
            else if (child.addedTrees.isEmpty() &&
                    child.removedTrees.isEmpty() &&
                    child.addedProperties.isEmpty() &&
                    child.removedProperties.isEmpty()) {
                    
                return Status.EXISTING;
            }
            else {
                return Status.MODIFIED;
            }
        }
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
    public Iterable<Tree> getChildren() {
        // Copy of removed children
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedTrees);

        // Copy od added and re-added children
        final Set<Tree> added = new HashSet<Tree>();
        added.addAll(addedTrees.values());

        // Filter removed child node entries from persisted child node entries,
        // map remaining child node entries to content trees and add added children.
        return new Iterable<Tree>() {
            @Override
            public Iterator<Tree> iterator() {
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
                Iterator<Tree> persistedMinusRemoved =
                    map(persistedMinusRemovedEntries,
                        new Function1<ChildNodeEntry, Tree>() {
                            @Override
                            public Tree apply(ChildNodeEntry entry) {
                                return getExistingChild(entry.getName());
                            }
                        });

                // persisted trees - removed trees + added trees
                return chain(persistedMinusRemoved, added.iterator());
            }
        };
    }

    @Override
    public Tree addChild(String name) {
        if (!hasChild(name)) {
            addedTrees.put(name, new TreeImpl(this, name, listener));
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
    public void setProperty(String name, CoreValue value) {
        PropertyState propertyState = new PropertyStateImpl(name, value);
        setProperty(propertyState);
        if (listener != null) {
            listener.setProperty(this, name, value);
        }
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        PropertyState propertyState = new PropertyStateImpl(name, values);
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
     * @return  {@code true} if successful, {@code false otherwise}. I.e.
     * when {@code destName} already exists at {@code destParent}
     */
    public boolean move(TreeImpl destParent, String destName) {
        if (destParent.hasChild(destName)) {
            return false;
        }

        parent.markTreeRemoved(name);

        TreeImpl oldParent = parent;
        String oldName = name;

        name = destName;
        parent = destParent;
        destParent.addedTrees.put(destName, this);
        if (listener != null) {
            listener.move(oldParent, oldName, this);
        }

        return true;
    }

    /**
     * Copy this tree to the parent at {@code destParent} with the name {@code destName}.
     *
     * @param destParent  parent for the copied tree
     * @param destName  name for the copied tree
     * @return  {@code true} if successful, {@code false otherwise}. I.e.
     * when {@code destName} already exists at {@code destParent}
     */
    public boolean copy(TreeImpl destParent, String destName) {
        if (destParent.hasChild(destName)) {
            return false;
        }

        TreeImpl copy = new TreeImpl(this, destParent, destName);
        destParent.addedTrees.put(destName, copy);
        if (listener != null) {
            listener.copy(parent, name, copy);
        }
        return true;
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
    private TreeImpl getExistingChild(String name) {
        if (persistentState == null) {
            return null;
        }

        TreeImpl transientState = existingChildren.get(name);
        if (transientState == null) {
            NodeState state = persistentState.getChildNode(name);
            if (state == null) {
                return null;
            }
            transientState = new TreeImpl(state, this, name, listener);
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
