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
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ReadOnlyTree implements Tree {
	
    /**
     * Internal and hidden property that contains the child order
     */
    public static final String OAK_CHILD_ORDER = ":childOrder";

    /**
     * Parent of this tree, {@code null} for the root
     */
    private final ReadOnlyTree parent;

    /**
     * Name of this tree
     */
    private final String name;

    /**
     * Underlying node state
     */
    final NodeState state;

    public ReadOnlyTree(@Nonnull NodeState rootState) {
        this(null, "", rootState);
    }

    public ReadOnlyTree(@Nullable ReadOnlyTree parent, @Nonnull String name, @Nonnull NodeState state) {
        this.parent = parent;
        this.name = checkNotNull(name);
        this.state = checkNotNull(state);
        checkArgument(!name.isEmpty() || parent == null);
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
        if (isRoot()) {
            // shortcut
            return "/";
        }

        StringBuilder sb = new StringBuilder();
        buildPath(sb);
        return sb.toString();
    }

    private void buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    @Nonnull
    @Override
    public Tree getParent() {
        checkState(parent != null, "root tree does not have a parent");
        return parent;
    }

    @Override
    public PropertyState getProperty(String name) {
        return state.getProperty(name);
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
    public boolean hasProperty(String name) {
        return state.hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return state.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return state.getProperties();
    }

    @Nonnull
    @Override
    public ReadOnlyTree getChild(@Nonnull String name) {
        return new ReadOnlyTree(this, name, state.getChildNode(name));
    }

    @Override
    public Status getStatus() {
        return Status.EXISTING;
    }

    @Override
    public boolean exists() {
        return state.exists();
    }

    @Override
    public boolean hasChild(@Nonnull String name) {
        return state.hasChildNode(name);
    }

    @Override
    public long getChildrenCount() {
        return state.getChildNodeCount();
    }

    /**
     * This implementation does not respect ordered child nodes, but always
     * returns them in some implementation specific order.
     * <p/>
     *
     * @return the children.
     */
    @Override
    public Iterable<Tree> getChildren() {
        Iterable<String> childNames;
        if (hasOrderableChildren()) {
            childNames = getOrderedChildNames();
        } else {
            childNames = state.getChildNodeNames();
        }
        return transform(
                filter(childNames, new Predicate<String>() {
                    @Override
                    public boolean apply(String name) {
                        return !isHidden(name);
                    }
                }),
                createChild());
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
    public boolean remove() {
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

    @Override
    public String toString() {
        return getPath() + ": " + state;
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull
    String getIdentifier() {
        PropertyState property = state.getProperty(JcrConstants.JCR_UUID);
        if (property != null) {
            return property.getValue(STRING);
        } else if (parent == null) {
            return "/";
        } else {
            return PathUtils.concat(parent.getIdentifier(), name);
        }
    }
    
    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    private boolean hasOrderableChildren() {
        return state.hasProperty(OAK_CHILD_ORDER);
    }
    
    /**
     * Returns the ordered child names. This method must only be called when
     * this tree {@link #hasOrderableChildren()}.
     *
     * @return the ordered child names.
     */
    private Iterable<String> getOrderedChildNames() {
        assert hasOrderableChildren();
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    final PropertyState childOrder = state.getProperty(OAK_CHILD_ORDER);
                    int index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < childOrder.count();
                    }

                    @Override
                    public String next() {
                        return childOrder.getValue(STRING, index++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    protected Function<String, Tree> createChild(){
    	return new Function<String, Tree>() {
            @Override
            public Tree apply(String name) {
                return new ReadOnlyTree(ReadOnlyTree.this, name, state.getChildNode(name));
            }
        };
    }

}
