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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of tree based on {@link NodeStateBuilder}s. Each subtree
 * has an associated node state builder which is used for building the new
 * trees resulting from calling mutating methods.
 */
public class TreeImpl implements Tree {

    /** Underlying store */
    private final NodeStore store;

    private final NodeStateBuilder rootBuilder;

    /**
     * Underlying persistent state or {@code null} if this instance represents an
     * added tree
     */
    private final NodeState baseState;

    /** Listener for changes on this tree */
    private final Listener listener;

    /** Parent of this tree */
    private TreeImpl parent;

    /** Name of this tree */
    private String name;

    // FIXME: should be synchronized, and weak refs
    private final Map<String, TreeImpl> children = new HashMap<String, TreeImpl>();

    private TreeImpl(NodeStore store, NodeState baseState, NodeStateBuilder rootBuilder,
            TreeImpl parent, String name, Listener listener) {

        this.store = store;
        this.rootBuilder = rootBuilder;
        this.baseState = baseState;
        this.listener = listener;
        this.parent = parent;
        this.name = name;
    }

    /**
     * Create a new instance which represents the root of a tree.
     * @param store  underlying store to the tree
     * @param rootBuilder  builder for the root
     * @param listener  change listener for the tree. May be {@code null} if
     *                  listening to changes is not needed.
     */
    TreeImpl(NodeStore store, NodeStateBuilder rootBuilder, Listener listener) {
        this(store, rootBuilder.getNodeState(), rootBuilder, null, "", listener);
    }

    /**
     * Listener for changes on {@code Tree}s
     */
    interface Listener {

        /**
         * The child of the given {@code name} has been added to {@code tree}.
         * @param parent  parent to which a child was added
         * @param name  name of the added child
         */
        void addChild(TreeImpl parent, String name);

        /**
         * The child of the given {@code name} has been removed from {@code tree}
         * @param parent  parent from which a child was removed
         * @param name  name of the removed child
         */
        void removeChild(TreeImpl parent, String name);

        /**
         * The property of the given {@code name} and {@code value} has been set.
         * @param parent  parent on which the property was set.
         * @param name  name of the property
         * @param value  value of the property
         */
        void setProperty(TreeImpl parent, String name, CoreValue value);

        /**
         * The property of the given {@code name} and {@code values} has been set.
         * @param parent  parent on which the property was set.
         * @param name  name of the property
         * @param values  values of the property
         */
        void setProperty(TreeImpl parent, String name, List<CoreValue> values);

        /**
         * The property of the given {@code name} has been removed.
         * @param parent  parent on which the property was removed.
         * @param name  name of the property
         */
        void removeProperty(TreeImpl parent, String name);

        /**
         * The child with the given {@code name} has been moved.
         * @param sourceParent  parent from which the child was moved
         * @param sourceName  name of the moved child
         * @param moved  moved child
         */
        void move(TreeImpl sourceParent, String sourceName, TreeImpl moved);

        /**
         * The child with the given {@code name} been copied.
         * @param sourceParent  parent from which the child way copied
         * @param sourceName  name of the copied child
         * @param copied  copied child
         */
        void copy(TreeImpl sourceParent, String sourceName, TreeImpl copied);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPath() {
        // Shortcut for root
        if (parent == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        buildPath(sb);
        return sb.toString();
    }

    @Override
    public Tree getParent() {
        return parent;
    }

    @Override
    public PropertyState getProperty(String name) {
        return getNodeState().getProperty(name);
    }

    @Override
    public Status getPropertyStatus(String name) {
        if (baseState == null) {
            // This instance is NEW...
            if (hasProperty(name)) {
                // ...so all children are new
                return Status.NEW;
            }
            else {
                // ...unless they don't exist.
                return null;
            }
        }
        else {
            if (hasProperty(name)) {
                // We have the property...
                if (baseState.getProperty(name) == null) {
                    // ...but didn't have it before. So its NEW.
                    return Status.NEW;
                }
                else {
                    // ... and did have it before. So...
                    PropertyState base = baseState.getProperty(name);
                    PropertyState head = getProperty(name);
                    if (base.equals(head)) {
                        // ...it's EXISTING if it hasn't changed
                        return Status.EXISTING;
                    }
                    else {
                        // ...and MODIFIED otherwise.
                        return Status.MODIFIED;
                    }
                }
            }
            else {
                // We don't have the property
                if (baseState.getProperty(name) == null) {
                    // ...and didn't have it before. So it doesn't exist.
                    return null;
                }
                else {
                    // ...and didn't have it before. So it's REMOVED
                    return Status.REMOVED;
                }
            }
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return getNodeState().getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        return getNodeState().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return getNodeState().getProperties();
    }

    @Override
    public TreeImpl getChild(String name) {
        TreeImpl child = children.get(name);
        if (child != null) {
            return child;
        }

        if (!hasChild(name)) {
            return null;
        }

        NodeState childBaseState = baseState == null
                ? null
                : baseState.getChildNode(name);

        child = new TreeImpl(store, childBaseState, rootBuilder, this, name, listener);
        children.put(name, child);
        return child;
    }

    @Override
    public Status getChildStatus(String name) {
        if (baseState == null) {
            // This instance is NEW...
            if (hasChild(name)) {
                // ...so all children are new
                return Status.NEW;
            }
            else {
                // ...unless they don't exist.
                return null;
            }
        }
        else {
            if (hasChild(name)) {
                // We have the child...
                if (baseState.getChildNode(name) == null) {
                    // ...but didn't have it before. So its NEW.
                    return Status.NEW;
                }
                else {
                    // ... and did have it before. So...
                    if (isSame(baseState.getChildNode(name), getNodeState().getChildNode(name))) {
                        // ...it's EXISTING if it hasn't changed
                        return Status.EXISTING;
                    }
                    else {
                        // ...and MODIFIED otherwise.
                        return Status.MODIFIED;
                    }
                }
            }
            else {
                // We don't have the child
                if (baseState.getChildNode(name) == null) {
                    // ...and didn't have it before. So it doesn't exist.
                    return null;
                }
                else {
                    // ...and didn't have it before. So it's REMOVED
                    return Status.REMOVED;
                }
            }
        }
    }

    @Override
    public boolean hasChild(String name) {
        return getNodeState().getChildNode(name) != null;
    }

    @Override
    public long getChildrenCount() {
        return getNodeState().getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        return new Iterable<Tree>() {
            @Override
            public Iterator<Tree> iterator() {
                final NodeState nodeState = getNodeState();

                Iterator<? extends ChildNodeEntry> childEntries =
                        nodeState.getChildNodeEntries().iterator();

                return Iterators.map(childEntries, new Function1<ChildNodeEntry, Tree>() {
                    @Override
                    public Tree apply(ChildNodeEntry entry) {
                        String childName = entry.getName();
                        TreeImpl child = children.get(entry.getName());
                        if (child != null) {
                            return child;
                        }

                        NodeState childNodeState = nodeState.getChildNode(childName);
                        child = new TreeImpl(store, childNodeState, rootBuilder, TreeImpl.this, childName, listener);
                        children.put(childName, child);
                        return child;
                    }
                });
            }
        };
    }

    @Override
    public Tree addChild(String name) {
        if (getBuilder().addNode(name) != null) {
            listener.addChild(this, name);
        }
        TreeImpl child = getChild(name);
        children.put(name, child);
        return child;
    }

    @Override
    public boolean removeChild(String name) {
        boolean result = getBuilder().removeNode(name);
        if (result) {
            listener.removeChild(this, name);
            children.remove(name);
        }
        return result;
    }

    @Override
    public PropertyState setProperty(String name, CoreValue value) {
        PropertyState property = getBuilder().setProperty(name, value);
        if (listener != null) {
            listener.setProperty(this, name, value);
        }
        return property;
    }

    @Override
    public PropertyState setProperty(String name, List<CoreValue> values) {
        PropertyState property = getBuilder().setProperty(name, values);
        if (listener != null) {
            listener.setProperty(this, name, values);
        }
        return property;
    }

    @Override
    public void removeProperty(String name) {
        getBuilder().removeProperty(name);
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
        NodeStateBuilder builder = getBuilder();
        NodeStateBuilder destParentBuilder = destParent.getBuilder();
        boolean result = builder.moveTo(destParentBuilder, destName);
        if (result) {
            parent.children.remove(name);
            destParent.children.put(destName, this);

            TreeImpl oldParent = parent;
            String oldName = name;

            name = destName;
            parent = destParent;

            if (listener != null) {
                listener.move(oldParent, oldName, this);
            }
        }
        return result;
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
        boolean result = getBuilder().copyTo(destParent.getBuilder(), destName);
        if (result) {
            if (listener != null) {
                listener.copy(parent, name, destParent.getChild(destName));
            }
            return true;
        }
        return result;
    }

    //------------------------------------------------------------< private >---

    private void buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(name);
        }
    }

    private NodeStateBuilder getBuilder() {
        NodeStateBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(getPath())) {
            builder = builder.getChildBuilder(name);
            if (builder == null) {
                throw new IllegalStateException("Stale NodeStateBuilder for " + getPath());
            }
        }

        return builder;
    }

    private NodeState getNodeState() {
        return getBuilder().getNodeState();
    }

    private boolean isSame(NodeState state1, NodeState state2) {
        final boolean[] isDirty = {false};
        store.compare(state1, state2, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                isDirty[0] = true;
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                isDirty[0] = true;
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                isDirty[0] = true;
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                isDirty[0] = true;
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                // cut transitivity here
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                isDirty[0] = true;
            }
        });

        return !isDirty[0];
    }

}
