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
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.MutableRoot.Move;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractMutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

final class MutableTree extends AbstractMutableTree {

    /**
     * Underlying {@code Root} of this {@code Tree} instance
     */
    private final MutableRoot root;

    /**
     * Parent of this tree. Null for the root.
     */
    private MutableTree parent;

    /**
     * Name of the tree
     */
    private String name;

    private NodeBuilder nodeBuilder;

    /** Pointer into the list of pending moves */
    private Move pendingMoves;

    MutableTree(@Nonnull MutableRoot root, @Nonnull NodeBuilder nodeBuilder,
            @Nonnull Move pendingMoves) {
        this(root, pendingMoves, null, nodeBuilder, "");
    }

    private MutableTree(@Nonnull MutableRoot root, @Nonnull Move pendingMoves,
            @Nullable MutableTree parent, @Nonnull NodeBuilder nodeBuilder, @Nonnull String name) {
        this.root = checkNotNull(root);
        this.parent = parent;
        this.name = checkNotNull(name);
        this.nodeBuilder = nodeBuilder;
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    //------------------------------------------------------------< AbstractMutableTree >---

    @Override
    @CheckForNull
    protected AbstractMutableTree getParentOrNull() {
        return parent;
    }

    @Nonnull
    @Override
    protected NodeBuilder getNodeBuilder() {
        return nodeBuilder;
    }

    //-----------------------------------------------------< AbstractTree >---

    @Override
    @Nonnull
    protected MutableTree createChild(@Nonnull String name) throws IllegalArgumentException {
        return new MutableTree(root, pendingMoves, this,
                nodeBuilder.getChildNode(checkNotNull(name)), name);
    }

    //------------------------------------------------------------< Tree >---

    @Override
    @Nonnull
    public String getName() {
        beforeRead();
        return name;
    }

    @Override
    @Nonnull
    public String getPath() {
        beforeRead();
        return super.getPath();
    }

    @Override
    @Nonnull
    public Status getStatus() {
        beforeRead();
        return super.getStatus();
    }

    @Override
    public boolean exists() {
        beforeRead();
        return super.exists();
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        beforeRead();
        return super.getProperty(checkNotNull(name));
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        beforeRead();
        return super.hasProperty(checkNotNull(name));
    }

    @Override
    public long getPropertyCount() {
        beforeRead();
        return super.getPropertyCount();
    }

    @Override
    @CheckForNull
    public Status getPropertyStatus(@Nonnull String name) {
        beforeRead();
        return super.getPropertyStatus(checkNotNull(name));
    }

    @Override
    @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        beforeRead();
        return super.getProperties();
    }

    @Override
    @Nonnull
    public Tree getChild(@Nonnull String name) {
        beforeRead();
        return super.getChild(checkNotNull(name));
    }

    @Override
    public boolean hasChild(@Nonnull String name) {
        beforeRead();
        return super.hasChild(checkNotNull(name));
    }

    @Override
    public long getChildrenCount(long max) {
        beforeRead();
        return super.getChildrenCount(max);
    }

    @Override
    @Nonnull
    public Iterable<Tree> getChildren() {
        beforeRead();
        return super.getChildren();
    }

    @Override
    public boolean remove() {
        beforeWrite();
        boolean success = super.remove();
        if (success) {
            root.updated();
        }
        return success;
    }

    @Override
    @Nonnull
    public Tree addChild(@Nonnull String name) {
        beforeWrite();
        Tree child;
        if (hasChild(name)) {
            child = createChild(name);
        } else {
            child = super.addChild(name);
            root.updated();
        }
        return child;
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        beforeWrite();
        super.setOrderableChildren(enable);
    }

    @Override
    public boolean orderBefore(@Nullable String name) {
        beforeWrite();
        boolean success = super.orderBefore(name);
        if (success) {
            root.updated();
        }
        return success;
    }

    @Override
    public void setProperty(@Nonnull PropertyState property) {
        beforeWrite();
        super.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(@Nonnull String name, @Nonnull T value) {
        beforeWrite();
        super.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(@Nonnull String name, @Nonnull T value, @Nonnull Type<T> type) {
        beforeWrite();
        super.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(@Nonnull String name) {
        beforeWrite();
        super.removeProperty(name);
        root.updated();
    }

    //---------------------------------------------------------< internal >---
    /**
     * Set the parent and name of this tree.
     * @param parent  parent of this tree
     * @param name  name of this tree
     */
    void setParentAndName(@Nonnull MutableTree parent, @Nonnull String name) {
        this.name = checkNotNull(name);
        this.parent = checkNotNull(parent);
    }

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code newName}.
     * @param newParent new parent for this tree
     * @param newName   new name for this tree
     */
    boolean moveTo(@Nonnull MutableTree newParent, @Nonnull String newName) {
        name = checkNotNull(newName);
        parent = checkNotNull(newParent);
        boolean success = nodeBuilder.moveTo(newParent.nodeBuilder, newName);
        if (success) {
            parent.updateChildOrder(false);
            newParent.updateChildOrder(false);
        }
        return success;
    }

    /**
     * Get a possibly non existing tree.
     * @param path the path to the tree
     * @return a {@link Tree} instance for the child at {@code path}.
     */
    @Nonnull
    MutableTree getTree(@Nonnull String path) {
        checkArgument(isAbsolute(checkNotNull(path)));
        beforeRead();
        MutableTree child = this;
        for (String name : elements(path)) {
            child = new MutableTree(root, pendingMoves, child, child.nodeBuilder.getChildNode(name), name);
        }
        return child;
    }

    @Nonnull
    String getPathInternal() {
        if (parent == null) {
            return "/";
        } else {
            StringBuilder sb = new StringBuilder();
            buildPath(sb);
            return sb.toString();
        }
    }

    @Override
    protected void buildPath(@Nonnull StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(checkNotNull(sb));
            sb.append('/').append(name);
        }
    }

    //------------------------------------------------------------< private >---

    private void reconnect() {
        if (parent != null) {
            parent.reconnect();
            nodeBuilder = parent.nodeBuilder.getChildNode(name);
        }
    }

    /**
     * Verifies that this session is still alive and applies any pending
     * moves that might affect this node. This method needs to be called
     * at the beginning of all public read-only {@link Tree} methods to
     * guarantee a consistent view of the tree. See {@link #beforeWrite()}
     * for the equivalent method for write operations.
     *
     * @throws IllegalStateException if this session is closed
     */
    private void beforeRead() throws IllegalStateException {
        root.checkLive();
        if (applyPendingMoves()) {
            reconnect();
        }
    }

    /**
     * Like {@link #beforeRead()} but also checks that (after any pending
     * moves have been applied) the current node exists and is visible.
     * This method needs to be called at the beginning of all public
     * {@link Tree} methods that modify this node to guarantee a consistent
     * view of the tree and to throw an exception whenever there's an
     * attempt to modify a missing node.
     *
     * @throws IllegalStateException if this node does not exist or
     *                               if this session is closed
     */
    private void beforeWrite() throws IllegalStateException {
        beforeRead();
        if (!super.exists()) {
            throw new IllegalStateException("This tree does not exist");
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

}
