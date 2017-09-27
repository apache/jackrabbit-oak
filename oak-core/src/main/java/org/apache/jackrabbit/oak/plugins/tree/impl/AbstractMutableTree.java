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

package org.apache.jackrabbit.oak.plugins.tree.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * {@code AbstractMutableTree} extends {@code AbstractTree} with implementations
 * for most write methods of {@code Tree}. Furthermore it handles the ordering
 * of siblings.
 */
public abstract class AbstractMutableTree extends AbstractTree {

    @Override
    public boolean remove() {
        String name = getName();
        AbstractTree parent = getParentOrNull();
        if (parent != null && parent.hasChild(name)) {
            getNodeBuilder().remove();
            NodeBuilder parentBuilder = parent.getNodeBuilder();
            PropertyState order = parentBuilder.getProperty(OAK_CHILD_ORDER);
            if (order != null) {
                List<String> names = newArrayListWithCapacity(order.count());
                for (String n : order.getValue(NAMES)) {
                    if (!n.equals(name)) {
                        names.add(n);
                    }
                }
                parentBuilder.setProperty(OAK_CHILD_ORDER, names, NAMES);
            }
            return true;
        } else {
            return false;
        }
    }

    @Nonnull
    @Override
    public Tree addChild(@Nonnull String name) throws IllegalArgumentException {
        checkArgument(!isHidden(name));
        if (!hasChild(name)) {
            NodeBuilder nodeBuilder = getNodeBuilder();
            nodeBuilder.setChildNode(name);
            PropertyState order = nodeBuilder.getProperty(OAK_CHILD_ORDER);
            if (order != null) {
                List<String> names = newArrayListWithCapacity(order.count() + 1);
                for (String n : order.getValue(NAMES)) {
                    if (!n.equals(name)) {
                        names.add(n);
                    }
                }
                names.add(name);
                nodeBuilder.setProperty(OAK_CHILD_ORDER, names, NAMES);
            }
        }
        return createChild(name);
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        if (enable) {
            updateChildOrder(true);
        } else {
            getNodeBuilder().removeProperty(OAK_CHILD_ORDER);
        }
    }

    /**
     * Updates the child order to match any added or removed child nodes that
     * are not yet reflected in the {@link TreeConstants#OAK_CHILD_ORDER}
     * property. If the {@code force} flag is set, the child order is set
     * in any case, otherwise only if the node already is orderable.
     *
     * @param force whether to add child order information if it doesn't exist
     */
    protected void updateChildOrder(boolean force) {
        if (force || hasOrderableChildren()) {
            getNodeBuilder().setProperty(PropertyStates.createProperty(
                    OAK_CHILD_ORDER, getChildNames(), Type.NAMES));
        }
    }

    @Override
    public boolean orderBefore(@Nullable String name) {
        String thisName = getName();
        AbstractTree parent = getParentOrNull();
        if (parent == null) {
            return false; // root does not have siblings
        } else if (thisName.equals(name)) {
            return false; // same node
        }

        // perform the reorder
        List<String> names = newArrayListWithCapacity(10000);
        NodeBuilder builder = parent.getNodeBuilder();
        boolean found = false;

        // first try reordering based on the (potentially out-of-sync)
        // child order property in the parent node
        for (String n : builder.getNames(OAK_CHILD_ORDER)) {
            if (n.equals(name) && parent.hasChild(name)) {
                names.add(thisName);
                found = true;
            }
            if (!n.equals(thisName)) {
                names.add(n);
            }
        }

        // if the target node name was not found in the parent's child order
        // property, we need to fall back to recreating the child order list
        if (!found) {
            names.clear();
            for (String n : parent.getChildNames()) {
                if (n.equals(name)) {
                    names.add(thisName);
                    found = true;
                }
                if (!n.equals(thisName)) {
                    names.add(n);
                }
            }
        }

        if (name == null) {
            names.add(thisName);
            found = true;
        }

        if (found) {
            builder.setProperty(OAK_CHILD_ORDER, names, NAMES);
            return true;
        } else {
            // no such sibling (not existing or not accessible)
            return false;
        }
    }

    @Override
    public void setProperty(@Nonnull PropertyState property) {
        checkArgument(!isHidden(checkNotNull(property).getName()));
        getNodeBuilder().setProperty(property);
    }

    @Override
    public <T> void setProperty(@Nonnull String name, @Nonnull T value) throws IllegalArgumentException {
        checkArgument(!isHidden(checkNotNull(name)));
        getNodeBuilder().setProperty(name, checkNotNull(value));
    }

    @Override
    public <T> void setProperty(@Nonnull String name, @Nonnull T value, @Nonnull Type<T> type)
            throws IllegalArgumentException {
        checkArgument(!isHidden(checkNotNull(name)));
        getNodeBuilder().setProperty(name, checkNotNull(value), checkNotNull(type));
    }

    @Override
    public void removeProperty(@Nonnull String name) {
        getNodeBuilder().removeProperty(checkNotNull(name));
    }
}
