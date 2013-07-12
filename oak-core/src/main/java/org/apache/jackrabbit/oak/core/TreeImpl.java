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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static org.apache.jackrabbit.oak.api.Tree.Status.EXISTING;
import static org.apache.jackrabbit.oak.api.Tree.Status.MODIFIED;
import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import java.util.Collections;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.RootImpl.Move;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

public class TreeImpl extends AbstractTree {

    /**
     * Underlying {@code Root} of this {@code Tree} instance
     */
    private final RootImpl root;

    /**
     * Parent of this tree. Null for the root.
     */
    private TreeImpl parent;

    /** Pointer into the list of pending moves */
    private Move pendingMoves;

    TreeImpl(RootImpl root, NodeBuilder builder, Move pendingMoves) {
        super("", builder, false);
        this.root = checkNotNull(root);
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    private TreeImpl(RootImpl root, TreeImpl parent, String name, Move pendingMoves) {
        super(name, parent.nodeBuilder.getChildNode(name), false);
        this.root = checkNotNull(root);
        this.parent = checkNotNull(parent);
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    @Override
    protected TreeImpl createChild(String name) {
        return new TreeImpl(root, this, name, pendingMoves);
    }

    //------------------------------------------------------------< Tree >---

    @Override
    public String getName() {
        enter();
        return name;
    }

    @Override
    public String getPath() {
        enter();
        return super.getPath();
    }

    @Override
    public Status getStatus() {
        checkExists();
        return super.getStatus();
    }

    @Override
    public boolean exists() {
        return enter();
    }

    @Override
    public TreeImpl getParent() {
        checkState(parent != null, "root tree does not have a parent");
        root.checkLive();
        return parent;
    }

    @Override
    public PropertyState getProperty(String name) {
        enter();
        return super.getProperty(name);
    }

    @Override
    public boolean hasProperty(String name) {
        enter();
        return super.hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        enter();
        return super.getPropertyCount();
    }

    @Override
    public Status getPropertyStatus(String name) {
        // TODO: see OAK-212
        Status nodeStatus = getStatus();
        if (nodeStatus == NEW) {
            return (super.hasProperty(name)) ? NEW : null;
        }
        PropertyState head = super.getProperty(name);
        if (head == null) {
            return null;
        }

        PropertyState base = getSecureBase().getProperty(name);

        if (base == null) {
            return NEW;
        } else if (head.equals(base)) {
            return EXISTING;
        } else {
            return MODIFIED;
        }
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        enter();
        return super.getProperties();
    }

    @Override
    public Tree getChild(String name) {
        enter();
        return createChild(name);
    }

    @Override
    public boolean hasChild(String name) {
        enter();
        return super.hasChild(name);
    }

    @Override
    public long getChildrenCount() {
        enter();
        return super.getChildrenCount();
    }

    @Override
    public Iterable<Tree> getChildren() {
        enter();
        return super.getChildren();
    }

    @Override
    public boolean remove() {
        checkExists();
        if (parent != null && parent.hasChild(name)) {
            nodeBuilder.remove();
            if (parent.hasOrderableChildren()) {
                // FIXME (OAK-842) child order not updated when parent is not accessible
                parent.nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(STRING, parent.nodeBuilder.getProperty(OAK_CHILD_ORDER))
                                .removeValue(name)
                                .getPropertyState()
                );
            }
            root.updated();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Tree addChild(String name) {
        checkExists();
        if (!super.hasChild(name)) {
            nodeBuilder.setChildNode(name);
            if (hasOrderableChildren()) {
                nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(STRING, nodeBuilder.getProperty(OAK_CHILD_ORDER))
                                .addValue(name)
                                .getPropertyState());
            }
            root.updated();
        }
        return createChild(name);
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        checkExists();
        if (enable) {
            ensureChildOrderProperty();
        } else {
            nodeBuilder.removeProperty(OAK_CHILD_ORDER);
        }
    }

    @Override
    public boolean orderBefore(final String name) {
        checkExists();
        if (parent == null) {
            // root does not have siblings
            return false;
        }
        if (name != null && !parent.hasChild(name)) {
            // so such sibling or not accessible
            return false;
        }
        // perform the reorder
        parent.ensureChildOrderProperty();
        // all siblings but not this one
        Iterable<String> siblings = filter(
                parent.getChildNames(),
                new Predicate<String>() {
                    @Override
                    public boolean apply(String name) {
                        return !TreeImpl.this.name.equals(name);
                    }
                });
        // create head and tail
        Iterable<String> head;
        Iterable<String> tail;
        if (name == null) {
            head = siblings;
            tail = Collections.emptyList();
        } else {
            int idx = indexOf(siblings, new Predicate<String>() {
                @Override
                public boolean apply(String sibling) {
                    return name.equals(sibling);
                }
            });
            head = Iterables.limit(siblings, idx);
            tail = Iterables.skip(siblings, idx);
        }
        // concatenate head, this name and tail
        parent.nodeBuilder.setProperty(
                MultiStringPropertyState.stringProperty(
                        OAK_CHILD_ORDER, Iterables.concat(head, Collections.singleton(getName()), tail))
        );
        root.updated();
        return true;
    }

    @Override
    public void setProperty(PropertyState property) {
        checkExists();
        nodeBuilder.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        checkExists();
        nodeBuilder.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        checkExists();
        nodeBuilder.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(String name) {
        checkExists();
        nodeBuilder.removeProperty(name);
        root.updated();
    }

    @Override
    public String toString() {
        return getPathInternal() + ": " + getNodeState();
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code destName}.
     * @param destParent new parent for this tree
     * @param destName   new name for this tree
     */
    void moveTo(TreeImpl destParent, String destName) {
        name = destName;
        parent = destParent;
    }

    /**
     * Get a possibly non existing tree.
     * @param path the path to the tree
     * @return a {@link Tree} instance for the child at {@code path}.
     */
    @CheckForNull
    TreeImpl getTree(@Nonnull String path) {
        checkArgument(isAbsolute(checkNotNull(path)));
        TreeImpl child = this;
        for (String name : elements(path)) {
            child = new TreeImpl(root, child, name, pendingMoves);
        }
        return child;
    }

    /**
     * Update the child order with children that have been removed or added.
     * Added children are appended to the end of the {@link #OAK_CHILD_ORDER}
     * property.
     */
    void updateChildOrder() {
        if (!hasOrderableChildren()) {
            return;
        }
        Set<String> names = Sets.newLinkedHashSet();
        for (String name : getChildNames()) {
            if (nodeBuilder.hasChildNode(name)) {
                names.add(name);
            }
        }
        for (String name : nodeBuilder.getChildNodeNames()) {
            names.add(name);
        }
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(
                STRING, OAK_CHILD_ORDER);
        builder.setValues(names);
        nodeBuilder.setProperty(builder.getPropertyState());
    }

    String getPathInternal() {
        return super.getPath();
    }

    //------------------------------------------------------------< private >---

    private boolean reconnect() {
        if (parent != null && parent.reconnect()) {
            nodeBuilder = parent.nodeBuilder.getChildNode(name);
        }
        return nodeBuilder.exists();
    }

    private void checkExists() {
        checkState(enter(), "This tree does not exist");
    }

    private boolean enter() {
        root.checkLive();
        if (isHidden(name)) {
            return false;
        } else if (applyPendingMoves()) {
            return reconnect();
        } else {
            return nodeBuilder.exists();
        }
    }

    /**
     * The (possibly non-existent) node state this tree is based on.
     * @return the base node state of this tree
     */
    @Nonnull
    private NodeState getSecureBase() {
        if (parent == null) {
            return root.getSecureBase();
        } else {
            return parent.getSecureBase().getChildNode(name);
        }
    }

    private boolean applyPendingMoves() {
        boolean movesApplied = false;
        if (parent != null) {
            movesApplied = parent.applyPendingMoves();
        }
        Move old = pendingMoves;
        pendingMoves = pendingMoves.apply(this);
        if (pendingMoves != old) {
            movesApplied = true;
        }
        return movesApplied;
    }

    /**
     * Ensures that the {@link #OAK_CHILD_ORDER} exists. This method will create
     * the property if it doesn't exist and initialize the value with the names
     * of the children as returned by {@link NodeBuilder#getChildNodeNames()}.
     */
    private void ensureChildOrderProperty() {
        if (!nodeBuilder.hasProperty(OAK_CHILD_ORDER)) {
            nodeBuilder.setProperty(
                    MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, nodeBuilder.getChildNodeNames()));
        }
    }

}


