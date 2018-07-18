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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.ReadOnly;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeAware;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable implementation of the {@code Tree} interface in order to provide
 * the much feature rich API functionality for a given {@code NodeState}.
 *
 * <h3>Tree hierarchy</h3>
 * Due to the nature of this {@code Tree} implementation creating a proper
 * hierarchical view of the tree structure is the responsibility of the caller.
 * It is recommended to start with the state of the
 * {@link #ImmutableTree(org.apache.jackrabbit.oak.spi.state.NodeState) root node}
 * and build up the hierarchy by calling
 * {@link #ImmutableTree(ImmutableTree, String, org.apache.jackrabbit.oak.spi.state.NodeState)}
 * for every subsequent child state. Note, that this implementation will not
 * perform any kind of validation of the passed state and methods like {@link #isRoot()},
 * {@link #getName()} or {@link #getPath()} will just make use of the hierarchy that has been
 * create by that sequence. In order to create a disconnected individual tree in cases where
 * the hierarchy information is not (yet) need or known it is suggested to use
 * {@link #ImmutableTree(ImmutableTree.ParentProvider, String, org.apache.jackrabbit.oak.spi.state.NodeState)}
 * an specify an appropriate {@code ParentProvider} implementation.
 *
 * <h3>ParentProvider</h3>
 * Apart from create the tree hierarchy in traversal mode this tree implementation
 * allows to instantiate disconnected trees that depending on the use may
 * never or on demand retrieve hierarchy information. The following default
 * implementations of this internal interface are present:
 *
 * <ul>
 *     <li>{@link DefaultParentProvider}: used with the default usage where the
 *     parent tree is passed to the constructor</li>
 *     <li>{@link ParentProvider#ROOT_PROVIDER}: the default parent provider for
 *     the root tree. All children will get {@link DefaultParentProvider}</li>
 *     <li>{@link ParentProvider#UNSUPPORTED}: throws {@code UnsupportedOperationException}
 *     upon hierarchy related methods like {@link #getParent()}, {@link #getPath()}</li>
 * </ul>
 *
 * <h3>Filtering 'hidden' items</h3>
 * This {@code Tree} implementation reflects the item hierarchy as exposed by the
 * underlying {@code NodeState}. In contrast to the mutable implementations it
 * does not filter out 'hidden' items as identified by
 * {@code org.apache.jackrabbit.oak.spi.state.NodeStateUtils#isHidden(String)}.
 *
 * <h3>Equality and hash code</h3>
 * In contrast to {@link org.apache.jackrabbit.oak.plugins.tree.impl.AbstractMutableTree}
 * the {@code ImmutableTree} implements
 * {@link Object#equals(Object)} and {@link Object#hashCode()}: Two {@code ImmutableTree}s
 * are consider equal if their name and the underlying {@code NodeState}s are equal. Note
 * however, that according to the contract defined in {@code NodeState} these
 * objects are not expected to be used as hash keys.
 */
public final class ImmutableTree extends AbstractTree implements TreeTypeAware, ReadOnly {

    /**
     * Underlying node state
     */
    private final NodeBuilder nodeBuilder;

    /**
     * Name of this tree
     */
    private final String name;

    private final ParentProvider parentProvider;

    private String path;

    private TreeType type;

    public ImmutableTree(@NotNull NodeState rootState) {
        this(ParentProvider.ROOT_PROVIDER, PathUtils.ROOT_NAME, rootState);
    }

    public ImmutableTree(@NotNull ImmutableTree parent, @NotNull String name, @NotNull NodeState state) {
        this(new DefaultParentProvider(parent), name, state);
    }

    public ImmutableTree(@NotNull ParentProvider parentProvider, @NotNull String name, @NotNull NodeState state) {
        this.nodeBuilder = new ReadOnlyBuilder(state);
        this.name = name;
        this.parentProvider = parentProvider;
    }

    //----------------------------------------------------------< TypeAware >---
    @Nullable
    public TreeType getType() {
        return type;
    }

    public void setType(@NotNull TreeType type) {
        this.type = type;
    }

    //-------------------------------------------------------< AbstractTree >---
    @Override
    @NotNull
    protected ImmutableTree createChild(@NotNull String name) {
        return new ImmutableTree(this, name, nodeBuilder.getNodeState().getChildNode(name));
    }

    @Override
    public boolean isRoot() {
        return PathUtils.ROOT_NAME.equals(name);
    }

    @Override
    @Nullable
    protected AbstractTree getParentOrNull() {
        return parentProvider.getParent();
    }

    @NotNull
    @Override
    protected NodeBuilder getNodeBuilder() {
        return nodeBuilder;
    }

    @Override
    protected boolean isHidden(@NotNull String name) {
        return false;
    }

    @NotNull
    @Override
    protected String[] getInternalNodeNames() {
        return new String[0];
    }

    //---------------------------------------------------------------< Tree >---

    @NotNull
    @Override
    public String getName() {
        return name;
    }

    @Override
    @NotNull
    public String getPath() {
        if (path == null) {
            path = super.getPath();
        }
        return path;
    }

    @NotNull
    @Override
    public ImmutableTree getChild(@NotNull String name) throws IllegalArgumentException {
        return createChild(name);

    }

    @Override
    public boolean remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    @NotNull
    public Tree addChild(@NotNull String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean orderBefore(@Nullable String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(@NotNull PropertyState property) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setProperty(@NotNull String name, @NotNull T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setProperty(@NotNull String name, @NotNull T value, @NotNull Type<T> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeProperty(@NotNull String name) {
        throw new UnsupportedOperationException();
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        return Objects.hashCode(getName(), nodeBuilder.getNodeState());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ImmutableTree) {
            ImmutableTree other = (ImmutableTree) o;
            return getName().equals(other.getName()) && nodeBuilder.getNodeState().equals(other.nodeBuilder.getNodeState());
        }
        return false;
    }

    //--------------------------------------------------------------------------

    public interface ParentProvider {

        ParentProvider UNSUPPORTED = new ParentProvider() {
            @Override
            public ImmutableTree getParent() {
                throw new UnsupportedOperationException("not supported.");
            }
        };

        ParentProvider ROOT_PROVIDER = new ParentProvider() {
            @Override
            public ImmutableTree getParent() {
                return null;
            }
        };

        @Nullable
        ImmutableTree getParent();
    }

    public static final class DefaultParentProvider implements ParentProvider {
        private final ImmutableTree parent;

        DefaultParentProvider(@NotNull ImmutableTree parent) {
            this.parent = parent;
        }

        @Override
        public ImmutableTree getParent() {
            return parent;
        }
    }

}
