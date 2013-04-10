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

import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;

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
 * {@link #getName()}, {@link #getPath()} or {@link #getLocation()} will just
 * make use of the hierarchy that has been create by that sequence.
 * In order to create a disconnected individual tree in cases where the hierarchy
 * information is not (yet) need or known it is suggested to use
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
 *     <li>{@link ParentProvider#ROOTPROVIDER}: the default parent provider for
 *     the root tree. All children will get {@link DefaultParentProvider}</li>
 *     <li>{@link ParentProvider#UNSUPPORTED}: throws {@code UnsupportedOperationException}
 *     upon hierarchy related methods like {@link #getParent()}, {@link #getPath()} and
 *     {@link #getIdentifier()}</li>
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
 * In contrast to {@link TreeImpl} the {@code ImmutableTree} implements
 * {@link Object#equals(Object)} and {@link Object#hashCode()}: Two {@code ImmutableTree}s
 * are consider equal if their name and the underlying {@code NodeState}s are equal. Note
 * however, that according to the contract defined in {@code NodeState} these
 * objects are not expected to be used as hash keys.
 *
 * FIXME: merge with ReadOnlyTree
 */
public final class ImmutableTree extends ReadOnlyTree {

    private final ParentProvider parentProvider;
    private final TreeTypeProvider typeProvider;

    private String path;

    public ImmutableTree(@Nonnull NodeState rootState) {
        this(ParentProvider.ROOTPROVIDER, "", rootState, TreeTypeProvider.EMPTY);
    }

    public ImmutableTree(@Nonnull NodeState rootState, @Nonnull TreeTypeProvider typeProvider) {
        this(ParentProvider.ROOTPROVIDER, "", rootState, typeProvider);
    }

    public ImmutableTree(@Nonnull ImmutableTree parent, @Nonnull String name, @Nonnull NodeState state) {
        this(new DefaultParentProvider(parent), name, state, parent.typeProvider);
    }

    public ImmutableTree(@Nonnull ParentProvider parentProvider, @Nonnull String name, @Nonnull NodeState state) {
        this(parentProvider, name, state, TreeTypeProvider.EMPTY);
    }

    public ImmutableTree(@Nonnull ParentProvider parentProvider, @Nonnull String name,
                         @Nonnull NodeState state, @Nonnull TreeTypeProvider typeProvider) {
        super(null, name, state);
        this.parentProvider = checkNotNull(parentProvider);
        this.typeProvider = typeProvider;
    }

    public static ImmutableTree createFromRoot(@Nonnull Root root, @Nonnull TreeTypeProvider typeProvider) {
        if (root instanceof RootImpl) {
            return new ImmutableTree(((RootImpl) root).getBaseState(), typeProvider);
        } else if (root instanceof ImmutableRoot) {
            return ((ImmutableRoot) root).getTree("/");
        } else {
            throw new IllegalArgumentException("Unsupported Root implementation.");
        }
    }

    //---------------------------------------------------------------< Tree >---
    @Override
    public boolean isRoot() {
        return "".equals(getName());
    }

    @Override
    public String getPath() {
        if (path == null) {
            if (isRoot()) {
                // shortcut
                path = "/";
            } else {
                StringBuilder sb = new StringBuilder();
                ImmutableTree parent = getParent();
                sb.append(parent.getPath());
                if (!parent.isRoot()) {
                    sb.append('/');
                }
                sb.append(getName());
                path = sb.toString();
            }
        }
        return path;
    }

    @Override
    public ImmutableTree getParent() {
        return parentProvider.getParent();
    }

    @Override
    public ImmutableTree getChild(@Nonnull String name) {
        NodeState child = state.getChildNode(name);
        if (child.exists()) {
            return new ImmutableTree(this, name, child);
        } else {
            return null;
        }
    }

    /**
     * This implementation does not respect ordered child nodes, but always
     * returns them in some implementation specific order.
     * <p/>
     * TODO: respect orderable children (needed?)
     *
     * @return the children.
     */
    @Override
    public Iterable<Tree> getChildren() {
        return new Iterable<Tree>() {
            @Override
            public Iterator<Tree> iterator() {
                final Iterator<? extends ChildNodeEntry> iterator = state.getChildNodeEntries().iterator();
                return new Iterator<Tree>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Tree next() {
                        ChildNodeEntry entry = iterator.next();
                        return new ImmutableTree(
                                ImmutableTree.this,
                                entry.getName(), entry.getNodeState());
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
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
        return new StringBuilder().append("ImmutableTree '").append(getName()).append("':").append(state.toString()).toString();
    }


    //--------------------------------------------------------------------------
    public NodeState getNodeState() {
        return state;
    }

    public int getType() {
        return typeProvider.getType(this);
    }

    // TODO
    public static int getType(Tree tree) {
        if (tree instanceof ImmutableTree) {
            return ((ImmutableTree) tree).getType();
        } else {
            return TreeTypeProvider.TYPE_DEFAULT;
        }
    }

    @Override
    @Nonnull
    String getIdentifier() {
        PropertyState property = state.getProperty(JcrConstants.JCR_UUID);
        if (property != null) {
            return property.getValue(STRING);
        } else if (getParent().isRoot()) {
            return "/";
        } else {
            return PathUtils.concat(getParent().getIdentifier(), getName());
        }
    }

    //--------------------------------------------------------------------------
    public interface ParentProvider {

        ParentProvider UNSUPPORTED = new ParentProvider() {
            @Override
            public ImmutableTree getParent() {
                throw new UnsupportedOperationException("not supported.");
            }
        };

        ParentProvider ROOTPROVIDER = new ParentProvider() {
            @Override
            public ImmutableTree getParent() {
                return null;
            }
        };

        @CheckForNull
        ImmutableTree getParent();
    }

    public static final class DefaultParentProvider implements ParentProvider {

        private ImmutableTree parent;

        DefaultParentProvider(ImmutableTree parent) {
            this.parent = checkNotNull(parent);
        }

        @Override
        public ImmutableTree getParent() {
            return parent;
        }
    }
}
