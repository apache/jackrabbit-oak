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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.RootImpl.PurgeListener;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;

import static org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState.EMPTY_NODE;

public class TreeImpl implements Tree, PurgeListener {

    /** Underlying {@code Root} of this {@code Tree} instance */
    private final RootImpl root;

    /** Parent of this tree */
    private TreeImpl parent;

    /** Name of this tree */
    private String name;

    /** Lazily initialised {@code NodeStateBuilder} for the underlying node state */
    NodeStateBuilder nodeStateBuilder;

    private final Children children = new Children();

    private TreeImpl(RootImpl root, TreeImpl parent, String name) {
        assert root != null;
        assert name != null;

        this.root = root;
        this.parent = parent;
        this.name = name;
    }

    @Nonnull
    static TreeImpl createRoot(final RootImpl root) {
        return new TreeImpl(root, null, "") {
            @Override
            protected NodeState getBaseState() {
                return root.getBaseState();
            }

            @Override
            protected synchronized NodeStateBuilder getNodeStateBuilder() {
                if (nodeStateBuilder == null) {
                    nodeStateBuilder = root.createRootBuilder();
                    root.addListener(this);
                }
                return nodeStateBuilder;
            }
        };
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isRoot() {
        return parent == null;
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
        return getNodeStateBuilder().getProperty(name);
    }

    @Override
    public Status getPropertyStatus(String name) {
        NodeState baseState = getBaseState();
        if (baseState == null) {
            // This instance is NEW...
            if (hasProperty(name)) {
                // ...so all children are new
                return Status.NEW;
            } else {
                // ...unless they don't exist.
                return null;
            }
        } else {
            if (hasProperty(name)) {
                // We have the property...
                if (baseState.getProperty(name) == null) {
                    // ...but didn't have it before. So its NEW.
                    return Status.NEW;
                } else {
                    // ... and did have it before. So...
                    PropertyState base = baseState.getProperty(name);
                    PropertyState head = getProperty(name);
                    if (base == null ? head == null : base.equals(head)) {
                        // ...it's EXISTING if it hasn't changed
                        return Status.EXISTING;
                    } else {
                        // ...and MODIFIED otherwise.
                        return Status.MODIFIED;
                    }
                }
            } else {
                // We don't have the property
                if (baseState.getProperty(name) == null) {
                    // ...and didn't have it before. So it doesn't exist.
                    return null;
                } else {
                    // ...and didn't have it before. So it's REMOVED
                    return Status.REMOVED;
                }
            }
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public long getPropertyCount() {
        return getNodeStateBuilder().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return getNodeStateBuilder().getProperties();
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

        child = new TreeImpl(root, this, name);
        children.put(name, child);
        return child;
    }

    @Override
    public Status getStatus() {
        NodeState baseState = getBaseState();
        if (baseState == null) {
            // Did not exist before, so its NEW
            return Status.NEW;
        }
        else {
            // Did exit it before. So...
            if (isSame(baseState, getNodeState())) {
                // ...it's EXISTING if it hasn't changed
                return Status.EXISTING;
            }
            else {
                // ...and MODIFIED otherwise.
                return Status.MODIFIED;
            }
        }
    }

    @Override
    public boolean hasChild(String name) {
        return getNodeStateBuilder().hasChildNode(name);
    }

    @Override
    public long getChildrenCount() {
        return getNodeStateBuilder().getChildNodeCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        return Iterables.transform(
                getNodeStateBuilder().getChildNodeNames(),
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String input) {
                        TreeImpl child = children.get(input);
                        if (child == null) {
                            child = new TreeImpl(root, TreeImpl.this, input);
                            children.put(input, child);
                        }
                        return  child;
                    }
                });
    }

    @Override
    public Tree addChild(String name) {
        if (!hasChild(name)) {
            NodeStateBuilder builder = getNodeStateBuilder();
            builder.setNode(name, EMPTY_NODE);
            root.purge();
        }

        TreeImpl child = getChild(name);
        assert child != null;
        return child;
    }

    @Override
    public boolean remove() {
        if (!isRoot() && parent.hasChild(name)) {
            NodeStateBuilder builder = parent.getNodeStateBuilder();
            builder.removeNode(name);
            parent.children.remove(name);
            parent.root.purge();
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public PropertyState setProperty(String name, CoreValue value) {
        NodeStateBuilder builder = getNodeStateBuilder();
        builder.setProperty(name, value);
        root.purge();
        PropertyState property = getProperty(name);
        assert property != null;
        return property;
    }

    @Override
    public PropertyState setProperty(String name, List<CoreValue> values) {
        NodeStateBuilder builder = getNodeStateBuilder();
        builder.setProperty(name, values);
        root.purge();
        PropertyState property = getProperty(name);
        assert property != null;
        return property;
    }

    @Override
    public void removeProperty(String name) {
        NodeStateBuilder builder = getNodeStateBuilder();
        builder.removeProperty(name);
        root.purge();
    }

    //--------------------------------------------------< RootImpl.Listener >---

    @Override
    public void purged() {
        nodeStateBuilder = null;
    }

    //----------------------------------------------------------< protected >---

    @CheckForNull
    protected NodeState getBaseState() {
        NodeState parentBaseState = parent.getBaseState();
        return parentBaseState == null
            ? null
            : parentBaseState.getChildNode(name);
    }

    @Nonnull
    protected synchronized NodeStateBuilder getNodeStateBuilder() {
        if (nodeStateBuilder == null) {
            nodeStateBuilder = parent.getNodeStateBuilder().getChildBuilder(name);
            root.addListener(this);
        }
        return nodeStateBuilder;
    }

    //------------------------------------------------------------< internal >---

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     *
     * @param destParent  new parent for this tree
     * @param destName  new name for this tree
     */
    void moveTo(TreeImpl destParent, String destName) {
        parent.children.remove(name);
        destParent.children.put(destName, this);

        name = destName;
        parent = destParent;
    }

    @Nonnull
    NodeState getNodeState() {
        return getNodeStateBuilder().getNodeState();
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

    private static boolean isSame(NodeState state1, NodeState state2) {
        final boolean[] isDirty = {false};
        state2.compareAgainstBaseState(state1, new NodeStateDiff() {
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

    private static class Children implements Iterable<TreeImpl> {

        private final Map<String, TreeImpl> children =
                CacheBuilder.newBuilder().weakValues().<String, TreeImpl>build().asMap();

        private final Lock readLock;
        private final Lock writeLock;

        {
            ReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
        }

        public void put(String name, TreeImpl tree) {
            writeLock.lock();
            try {
                children.put(name, tree);
            } finally {
                writeLock.unlock();
            }
        }

        public TreeImpl get(String name) {
            readLock.lock();
            try {
                return children.get(name);
            } finally {
                readLock.unlock();
            }
        }

        public void remove(String name) {
            writeLock.lock();
            try {
                children.remove(name);
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public Iterator<TreeImpl> iterator() {
            return children.values().iterator();
        }
    }

}
