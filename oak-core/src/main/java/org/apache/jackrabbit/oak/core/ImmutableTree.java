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
package org.apache.jackrabbit.oak.core;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;

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
 * {@link #ImmutableTree(org.apache.jackrabbit.oak.core.ImmutableTree.ParentProvider, String, org.apache.jackrabbit.oak.spi.state.NodeState)}
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
 * <h3>TreeTypeProvider</h3>
 * For optimization purpose an Immutable tree will be associated with a
 * {@code TreeTypeProvider} that allows for fast detection of the following types
 * of Trees:
 *
 * <ul>
 *     <li>{@link TreeTypeProvider#TYPE_HIDDEN}: a hidden tree whose name starts with ":".
 *     Please note that the whole subtree of a hidden node is considered hidden.</li>
 *     <li>{@link TreeTypeProvider#TYPE_AC}: A tree that stores access control content
 *     and requires special access {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#READ_ACCESS_CONTROL permissions}.</li>
 *     <li>{@link TreeTypeProvider#TYPE_VERSION}: if a given tree is located within
 *     any of the version related stores defined by JSR 283. Depending on the
 *     permission evaluation implementation those items require special treatment.</li>
 *     <li>{@link TreeTypeProvider#TYPE_DEFAULT}: the default type for trees that don't
 *     match any of the upper types.</li>
 * </ul>
 *
 * <h3>Equality and hash code</h3>
 * In contrast to {@link MutableTree} the {@code ImmutableTree} implements
 * {@link Object#equals(Object)} and {@link Object#hashCode()}: Two {@code ImmutableTree}s
 * are consider equal if their name and the underlying {@code NodeState}s are equal. Note
 * however, that according to the contract defined in {@code NodeState} these
 * objects are not expected to be used as hash keys.
 */
public final class ImmutableTree extends AbstractTree {

    /**
     * Underlying node state
     */
    private final NodeState state;

    private final ParentProvider parentProvider;
    private final TreeTypeProvider typeProvider;

    private String path;
    private int type = TreeTypeProvider.TYPE_NONE;

    public ImmutableTree(@Nonnull NodeState rootState) {
        this(ParentProvider.ROOT_PROVIDER, "", rootState, TreeTypeProvider.EMPTY);
    }

    public ImmutableTree(@Nonnull NodeState rootState, @Nonnull TreeTypeProvider typeProvider) {
        this(ParentProvider.ROOT_PROVIDER, "", rootState, typeProvider);
    }

    public ImmutableTree(@Nonnull ImmutableTree parent, @Nonnull String name, @Nonnull NodeState state) {
        this(new DefaultParentProvider(parent), name, state, parent.typeProvider);
    }

    public ImmutableTree(@Nonnull ParentProvider parentProvider, @Nonnull String name, @Nonnull NodeState state) {
        this(parentProvider, name, state, TreeTypeProvider.EMPTY);
    }

    public ImmutableTree(@Nonnull ParentProvider parentProvider, @Nonnull String name,
                         @Nonnull NodeState state, @Nonnull TreeTypeProvider typeProvider) {
        super(name, new ReadOnlyBuilder(state));
        this.state = state;
        this.parentProvider = parentProvider;
        this.typeProvider = typeProvider;
    }

    @Override
    protected ImmutableTree createChild(String name) {
        return new ImmutableTree(this, name, state.getChildNode(name));
    }

    //---------------------------------------------------------------< Tree >---

    @Override
    public String getPath() {
        if (path == null) {
            path = super.getPath();
        }
        return path;
    }

    @Override
    public boolean hasChild(String name) {
        return state.hasChildNode(name);
    }

    @Override
    public Status getStatus() {
        return Status.EXISTING;
    }

    @Override
    protected boolean isNew() {
        return false;
    }

    @Override
    protected boolean isModified() {
        return false;
    }

    @Override
    public ImmutableTree getParent() {
        return parentProvider.getParent();
    }

    @Override
    public Status getPropertyStatus(String name) {
        if (hasProperty(name)) {
            return Status.EXISTING;
        } else {
            return null;
        }
    }

    @Override
    public ImmutableTree getChild(String name) {
        return createChild(name);
    }

    @Override
    public boolean remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Tree addChild(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean orderBefore(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(PropertyState property) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeProperty(String name) {
        throw new UnsupportedOperationException();
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        return Objects.hashCode(getName(), state);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ImmutableTree) {
            ImmutableTree other = (ImmutableTree) o;
            return getName().equals(other.getName()) && state.equals(other.state);
        }
        return false;
    }

    @Override
    public String toString() {
        return "ImmutableTree '" + getName() + "':" + state.toString();
    }

    //--------------------------------------------------------------------------

    public int getType() {
        if (type == TreeTypeProvider.TYPE_NONE) {
            type = typeProvider.getType(this);
        }
        return type;
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
                throw new IllegalStateException("root tree does not have a parent");
            }
        };

        @CheckForNull
        ImmutableTree getParent();
    }

    public static final class DefaultParentProvider implements ParentProvider {
        private final ImmutableTree parent;

        DefaultParentProvider(@Nonnull ImmutableTree parent) {
            this.parent = parent;
        }

        @Override
        public ImmutableTree getParent() {
            return parent;
        }
    }

}
