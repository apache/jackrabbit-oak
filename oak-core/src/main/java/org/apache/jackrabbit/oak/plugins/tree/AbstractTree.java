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
package org.apache.jackrabbit.oak.plugins.tree;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.oak.api.Tree.Status.MODIFIED;
import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.apache.jackrabbit.oak.api.Tree.Status.UNCHANGED;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code AbstractTree} provides default implementations for most
 * read methods of {@code Tree}. Furthermore it handles the
 * {@link #setOrderableChildren(boolean) ordering} of child nodes
 * and hides internal items.
 */
public abstract class AbstractTree implements Tree {

    // TODO: make this configurable
    private static final String[] INTERNAL_NODE_NAMES = {
            IndexConstants.INDEX_CONTENT_NODE_NAME,
            NodeReferenceConstants.REF_NAME,
            NodeReferenceConstants.WEAK_REF_NAME,
            ConflictAnnotatingRebaseDiff.CONFLICT};

    /**
     * Name of this tree
     */
    protected String name;

    /**
     * The {@code NodeBuilder} for the underlying node state
     */
    protected NodeBuilder nodeBuilder;

    /**
     * Create a new {@code AbstractTree} instance
     * @param name  name of the tree
     * @param nodeBuilder  {@code NodeBuilder} for the underlying node state
     */
    protected AbstractTree(@Nonnull String name, @Nonnull NodeBuilder nodeBuilder) {
        this.name = checkNotNull(name);
        this.nodeBuilder = checkNotNull(nodeBuilder);
    }

    /**
     * @return  the underlying {@code NodeState} of this tree
     */
    @Nonnull
    public NodeState getNodeState() {
        return nodeBuilder.getNodeState();
    }

    /**
     * Factory method for creating child trees
     * @param name  name of the child tree
     * @return child tree of this tree with the given {@code name}
     * @throws IllegalArgumentException if the given name string is empty
     *                                  or contains the forward slash character
     */
    @Nonnull
    protected abstract AbstractTree createChild(@Nonnull String name)
            throws IllegalArgumentException;

    /**
     * @return  {@code true} iff {@code getStatus() == Status.NEW}
     */
    protected boolean isNew() {
        return nodeBuilder.isNew();
    }

    /**
     * @return  {@code true} iff {@code getStatus() == Status.MODIFIED}
     */
    protected boolean isModified() {
        return nodeBuilder.isModified();
    }

    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    protected boolean hasOrderableChildren() {
        return nodeBuilder.hasProperty(OAK_CHILD_ORDER);
    }

    /**
     * Returns the list of child names considering its ordering
     * when the {@link TreeConstants#OAK_CHILD_ORDER} property is set.
     *
     * @return the list of child names.
     */
    @Nonnull
    protected Iterable<String> getChildNames() {
        PropertyState order = nodeBuilder.getProperty(OAK_CHILD_ORDER);
        if (order != null && order.getType() == NAMES) {
            Set<String> names = newLinkedHashSet(nodeBuilder.getChildNodeNames());
            List<String> ordered = newArrayListWithCapacity(names.size());
            for (String name : order.getValue(NAMES)) {
                // only include names of child nodes that actually exist
                if (names.remove(name)) {
                    ordered.add(name);
                }
            }
            // add names of child nodes that are not explicitly ordered
            ordered.addAll(names);
            return ordered;
        } else {
            return nodeBuilder.getChildNodeNames();
        }
    }

    //------------------------------------------------------------< Object >---

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getPath()).append(": ");

        sb.append('{');
        for (PropertyState p : getProperties()) {
            sb.append(' ').append(p).append(',');
        }
        for (String n : this.getChildNames()) {
            sb.append(' ').append(n).append( " = { ... },");
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append('}');

        return sb.toString();
    }

    //---------------------------------------------------------------< Tree >---

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isRoot() {
        return name.isEmpty();
    }

    @Override
    public String getPath() {
        if (isRoot()) {
            return "/";
        } else {
            StringBuilder sb = new StringBuilder();
            buildPath(sb);
            return sb.toString();
        }
    }

    protected void buildPath(StringBuilder sb) {
        if (!isRoot()) {
            getParent().buildPath(sb);
            sb.append('/').append(name);
        }
    }

    @Override
    public Status getStatus() {
        if (nodeBuilder.isNew()) {
            return NEW;
        } else if (nodeBuilder.isModified()) {
            return MODIFIED;
        } else {
            return UNCHANGED;
        }
    }

    @Override
    public boolean exists() {
        return nodeBuilder.exists() && !isHidden(name);
    }

    @Override
    public abstract AbstractTree getParent();

    @Override
    public PropertyState getProperty(String name) {
        return !isHidden(checkNotNull(name))
            ? nodeBuilder.getProperty(name)
            : null;
    }

    @Override
    public boolean hasProperty(String name) {
        return (!isHidden(checkNotNull(name))) && nodeBuilder.hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return size(getProperties());
    }

    @Override
    public Status getPropertyStatus(@Nonnull String name) {
        if (!hasProperty(name)) {
            return null;
        } else if (nodeBuilder.isNew(name)) {
            return NEW;
        } else if (nodeBuilder.isReplaced(name)) {
            return MODIFIED;
        } else {
            return UNCHANGED;
        }
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return filter(nodeBuilder.getProperties(),
            new Predicate<PropertyState>() {
                @Override
                public boolean apply(PropertyState propertyState) {
                    return !isHidden(propertyState.getName());
                }
            });
    }

    @Override
    public boolean hasChild(String name) {
        return nodeBuilder.hasChildNode(name) && !isHidden(name);
    }

    @Override
    public long getChildrenCount(long max) {
        if (max + INTERNAL_NODE_NAMES.length < 0) {
            // avoid overflow (if max is near Long.MAX_VALUE)
            max = Long.MAX_VALUE;
        } else {
            // fetch a few more
            max += INTERNAL_NODE_NAMES.length;
        }
        long count = nodeBuilder.getChildNodeCount(max);
        if (count > 0) {
            for (String name : INTERNAL_NODE_NAMES) {
                if (nodeBuilder.hasChildNode(name)) {
                    count--;
                }
            }
        }
        return count;
    }

    @Override
    public Iterable<Tree> getChildren() {
        Iterable<Tree> children = transform(getChildNames(),
            new Function<String, Tree>() {
                @Override
                public Tree apply(String name) {
                    AbstractTree child = createChild(name);
                    return child.exists() ? child : null;
                }
            });
        return filter(children, notNull());
    }
}
