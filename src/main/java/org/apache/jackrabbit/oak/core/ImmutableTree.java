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
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * ImmutableTree...
 * FIXME: merge with ReadOnlyTree
 */
public final class ImmutableTree extends ReadOnlyTree {

    private final ParentProvider parentProvider;
    private final TypeProvider typeProvider;

    private String path;

    public ImmutableTree(@Nonnull NodeState rootState) {
        this(ParentProvider.ROOTPROVIDER, "", rootState, TypeProvider.EMPTY);
    }

    public ImmutableTree(@Nonnull NodeState rootState, @Nonnull TypeProvider typeProvider) {
        this(ParentProvider.ROOTPROVIDER, "", rootState, typeProvider);
    }

    public ImmutableTree(@Nonnull ImmutableTree parent, @Nonnull String name, @Nonnull NodeState state) {
        this(new DefaultParentProvider(parent), name, state, parent.typeProvider);
    }

    public ImmutableTree(@Nonnull ParentProvider parentProvider, @Nonnull String name, @Nonnull NodeState state) {
        this(parentProvider, name, state, TypeProvider.EMPTY);
    }

    public ImmutableTree(@Nonnull ParentProvider parentProvider, @Nonnull String name,
                         @Nonnull NodeState state, @Nonnull TypeProvider typeProvider) {
        super(null, name, state);
        this.parentProvider = checkNotNull(parentProvider);
        this.typeProvider = typeProvider;
    }

    public static ImmutableTree createFromRoot(@Nonnull Root root, @Nonnull TypeProvider typeProvider) {
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
        NodeState child = getNodeState().getChildNode(name);
        if (child != null) {
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
                final Iterator<? extends ChildNodeEntry> iterator =
                        getNodeState().getChildNodeEntries().iterator();
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
            return TypeProvider.TYPE_DEFAULT;
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
    // TODO
    public interface TypeProvider {

        int TYPE_DEFAULT = 1;
        int TYPE_VERSION = 2;
        int TYPE_AC = 4;
        int TYPE_HIDDEN = 8;

        TypeProvider EMPTY = new TypeProvider() {
            @Override
            public int getType(@Nullable ImmutableTree tree) {
                return TYPE_DEFAULT;
            }
        };

        int getType(ImmutableTree tree);
    }

    public static final class DefaultTypeProvider implements TypeProvider {

        private final Context contextInfo;

        public DefaultTypeProvider(@Nonnull Context contextInfo) {
            this.contextInfo = contextInfo;
        }

        @Override
        public int getType(ImmutableTree tree) {
            ImmutableTree parent = tree.getParent();
            if (parent == null) {
                return TYPE_DEFAULT;
            }

            int type;
            switch (parent.getType()) {
                case TYPE_HIDDEN:
                    type = TYPE_HIDDEN;
                    break;
                case TYPE_VERSION:
                    type = TYPE_VERSION;
                    break;
                case TYPE_AC:
                    type = TYPE_AC;
                    break;
                default:
                    String name = tree.getName();
                    if (NodeStateUtils.isHidden(name)) {
                        type = TYPE_HIDDEN;
                    } else if (VersionConstants.VERSION_NODE_NAMES.contains(name) ||
                            VersionConstants.VERSION_NODE_TYPE_NAMES.contains(NodeStateUtils.getPrimaryTypeName(tree.getNodeState()))) {
                        type = TYPE_VERSION;
                    } else if (contextInfo.definesTree(tree)) {
                        type = TYPE_AC;
                    } else {
                        type = TYPE_DEFAULT;
                    }
            }
            return type;
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
