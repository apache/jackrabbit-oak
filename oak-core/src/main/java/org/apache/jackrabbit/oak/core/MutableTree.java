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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.MutableRoot.Move;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractMutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    MutableTree(@NotNull MutableRoot root, @NotNull NodeBuilder nodeBuilder,
            @NotNull Move pendingMoves) {
        this(root, pendingMoves, null, nodeBuilder, "");
    }

    private MutableTree(@NotNull MutableRoot root, @NotNull Move pendingMoves,
            @Nullable MutableTree parent, @NotNull NodeBuilder nodeBuilder, @NotNull String name) {
        this.root = requireNonNull(root);
        this.parent = parent;
        this.name = requireNonNull(name);
        this.nodeBuilder = nodeBuilder;
        this.pendingMoves = requireNonNull(pendingMoves);
    }

    //------------------------------------------------------------< AbstractMutableTree >---

    @Override
    @Nullable
    protected AbstractMutableTree getParentOrNull() {
        return parent;
    }

    @NotNull
    @Override
    protected NodeBuilder getNodeBuilder() {
        return nodeBuilder;
    }

    //-----------------------------------------------------< AbstractTree >---

    @Override
    @NotNull
    protected MutableTree createChild(@NotNull String name) throws IllegalArgumentException {
        return new MutableTree(root, pendingMoves, this,
                nodeBuilder.getChildNode(requireNonNull(name)), name);
    }

    //------------------------------------------------------------< Tree >---

    @Override
    @NotNull
    public String getName() {
        beforeRead();
        return name;
    }

    @Override
    @NotNull
    public String getPath() {
        beforeRead();
        return super.getPath();
    }

    @Override
    @NotNull
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
    public PropertyState getProperty(@NotNull String name) {
        beforeRead();
        return super.getProperty(requireNonNull(name));
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        beforeRead();
        return super.hasProperty(requireNonNull(name));
    }

    @Override
    public long getPropertyCount() {
        beforeRead();
        return super.getPropertyCount();
    }

    @Override
    @Nullable
    public Status getPropertyStatus(@NotNull String name) {
        beforeRead();
        return super.getPropertyStatus(requireNonNull(name));
    }

    @Override
    @NotNull
    public Iterable<? extends PropertyState> getProperties() {
        beforeRead();
        return super.getProperties();
    }

    @Override
    @NotNull
    public Tree getChild(@NotNull String name) {
        beforeRead();
        return super.getChild(requireNonNull(name));
    }

    @Override
    public boolean hasChild(@NotNull String name) {
        beforeRead();
        return super.hasChild(requireNonNull(name));
    }

    @Override
    public long getChildrenCount(long max) {
        beforeRead();
        return super.getChildrenCount(max);
    }

    @Override
    @NotNull
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
    @NotNull
    public Tree addChild(@NotNull String name) {
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
    public void setProperty(@NotNull PropertyState property) {
        beforeWrite();
        super.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(@NotNull String name, @NotNull T value) {
        beforeWrite();
        super.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(@NotNull String name, @NotNull T value, @NotNull Type<T> type) {
        beforeWrite();
        super.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(@NotNull String name) {
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
    void setParentAndName(@NotNull MutableTree parent, @NotNull String name) {
        this.name = requireNonNull(name);
        this.parent = requireNonNull(parent);
    }

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code newName}.
     * @param newParent new parent for this tree
     * @param newName   new name for this tree
     */
    boolean moveTo(@NotNull MutableTree newParent, @NotNull String newName) {
        requireNonNull(newName);
        requireNonNull(newParent);
        MutableTree oldParent = parent;
        boolean success = nodeBuilder.moveTo(newParent.nodeBuilder, newName);
        if (success) {
            name = newName;
            parent = newParent;
            oldParent.updateChildOrder(false);
            newParent.updateChildOrder(false);
        }
        return success;
    }

    /**
     * Get a possibly non existing tree.
     * @param path the path to the tree
     * @return a {@link Tree} instance for the child at {@code path}.
     */
    @NotNull
    MutableTree getTree(@NotNull String path) {
        checkArgument(isAbsolute(requireNonNull(path)));
        beforeRead();
        MutableTree child = this;
        for (String name : elements(path)) {
            child = new MutableTree(root, pendingMoves, child, child.nodeBuilder.getChildNode(name), name);
        }
        return child;
    }

    @NotNull
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
    protected void buildPath(@NotNull StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(requireNonNull(sb));
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
            throw new IllegalStateException("The tree for "  + super.getPath() + " does not exist");
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
