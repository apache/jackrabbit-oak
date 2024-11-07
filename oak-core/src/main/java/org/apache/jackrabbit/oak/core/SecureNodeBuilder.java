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

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.LazyValue;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.size;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;

class SecureNodeBuilder implements NodeBuilder {

    /**
     * Root builder, or {@code this} for the root builder itself.
     */
    private final SecureNodeBuilder rootBuilder;

    /**
     * Parent builder, or {@code null} for a root builder.
     */
    private final SecureNodeBuilder parent;

    /**
     * Name of this child node within the parent builder,
     * or {@code null} for a root builder.
     */
    private final String name;

    /**
     * Permissions provider for evaluating access rights to the underlying raw builder
     */
    private final LazyValue<PermissionProvider> permissionProvider;

    /**
     * Underlying node builder.
     */
    private final NodeBuilder builder;

    /**
     * Security context of this subtree, updated whenever the base revision
     * changes.
     */
    private TreePermission treePermission = null; // initialized lazily

    /**
     * Possibly outdated reference to the tree permission of the root
     * builder. Used to detect when the base state (and thus the security
     * context) of the root builder has changed, and trigger an update of
     * the tree permission of this builder.
     *
     * @see #treePermission
     */
    private TreePermission rootPermission = null; // initialized lazily

    /**
     * Create the {@code SecureNodeBuilder} for the root node.
     *
     * @param builder The {@code NodeBuilder} of the root node.
     * @param permissionProvider The {@code PermissionProvider} used to evaluation read access.
     */
    SecureNodeBuilder(
            @NotNull NodeBuilder builder,
            @NotNull LazyValue<PermissionProvider> permissionProvider) {
        this.rootBuilder = this;
        this.parent = null;
        this.name = null;
        this.permissionProvider = requireNonNull(permissionProvider);
        this.builder = requireNonNull(builder);
    }

    private SecureNodeBuilder(SecureNodeBuilder parent, String name) {
        this.rootBuilder = parent.rootBuilder;
        this.parent = parent;
        this.name = name;
        this.permissionProvider = parent.permissionProvider;
        this.builder = parent.builder.getChildNode(name);
    }

    @Override @NotNull
    public NodeState getBaseState() {
        return new SecureNodeState(builder.getBaseState(), getTreePermission());
    }

    @Override @NotNull
    public NodeState getNodeState() {
        return new SecureNodeState(builder.getNodeState(), getTreePermission());
    }

    @Override
    public boolean exists() {
        return builder.exists()
                && (builder.isReplaced() || getTreePermission().canRead());
    }

    @Override
    public boolean isNew() {
        return builder.isNew()
                || (builder.isReplaced() && !getTreePermission().canRead());
    }

    @Override
    public boolean isNew(String name) {
        return builder.isNew(name)
                || (builder.isReplaced(name)
                        && !getTreePermission().canRead(builder.getProperty(name)));
    }

    @Override
    public boolean isModified() {
        return builder.isModified();
    }

    @Override
    public boolean isReplaced() {
        return builder.isReplaced() && !isNew();
    }

    @Override
    public boolean isReplaced(String name) {
        return builder.isReplaced(name) && !isNew(name);
    }

    public void baseChanged() {
        Validate.checkState(parent == null);
        treePermission = null; // trigger re-evaluation
        rootPermission = null;
    }

    @Override
    public boolean remove() {
        return exists() && builder.remove();
    }


    @Override
    public boolean moveTo(@NotNull NodeBuilder newParent, @NotNull String newName) {
        return exists() && builder.moveTo(newParent, newName);
    }

    @Nullable
    @Override
    public PropertyState getProperty(String name) {
        PropertyState property = builder.getProperty(name);
        if (new ReadablePropertyPredicate().test(property)) {
            return property;
        } else {
            return null;
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public synchronized long getPropertyCount() {
        if (getTreePermission().canReadProperties() || isNew()) {
            return builder.getPropertyCount();
        } else {
            return size(filter(
                    builder.getProperties(),
                    new ReadablePropertyPredicate()::test));
        }
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        if (getTreePermission().canReadProperties() || isNew()) {
            return builder.getProperties();
        } else {
            return filter(
                    builder.getProperties(),
                    new ReadablePropertyPredicate()::test);
        }
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        PropertyState property = getProperty(name);
        return isType(property, BOOLEAN)  && property.getValue(BOOLEAN);
    }

    @Nullable
    @Override
    public String getString(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (isType(property, STRING)) {
            return property.getValue(STRING);
        } else {
            return null;
        }
    }

    @Nullable
    @Override
    public String getName(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (isType(property, NAME)) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @NotNull
    @Override
    public Iterable<String> getNames(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (isType(property, NAMES)) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    @NotNull
    @Override
    public NodeBuilder setProperty(@NotNull PropertyState property) {
        builder.setProperty(property);
        return this;
    }

    @NotNull
    @Override
    public <T> NodeBuilder setProperty(String name, @NotNull T value) {
        builder.setProperty(name, value);
        return this;
    }

    @NotNull
    @Override
    public <T> NodeBuilder setProperty(
            String name, @NotNull T value, Type<T> type) {
        builder.setProperty(name, value, type);
        return this;
    }

    @NotNull
    @Override
    public NodeBuilder removeProperty(String name) {
        if (hasProperty(name)) { // only remove properties that we can see
            builder.removeProperty(name);
        }
        return this;
    }

    @NotNull
    @Override
    public Iterable<String> getChildNodeNames() {
        return filter(
                builder.getChildNodeNames(),
                input -> input != null && getChildNode(input).exists());
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        if (builder.hasChildNode(name)) {
            return getChildNode(name).exists();
        } else {
            return false;
        }
    }

    @NotNull
    @Override
    public NodeBuilder child(@NotNull String name) {
        if (hasChildNode(name)) {
            return getChildNode(name);
        } else {
            return setChildNode(name);
        }
    }

    @NotNull
    @Override
    public NodeBuilder setChildNode(@NotNull String name) {
        builder.setChildNode(name);
        return new SecureNodeBuilder(this, name);
    }

    @NotNull
    @Override
    public NodeBuilder setChildNode(@NotNull String name, @NotNull NodeState nodeState) {
        builder.setChildNode(name, nodeState);
        return new SecureNodeBuilder(this, name);
    }

    @NotNull
    @Override
    public NodeBuilder getChildNode(@NotNull String name) {
        return new SecureNodeBuilder(this, name);
    }

    @Override
    public synchronized long getChildNodeCount(long max) {
        if (getTreePermission().canReadAll()) {
            return builder.getChildNodeCount(max);
        } else {
            return size(getChildNodeNames());
        }
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return builder.createBlob(stream);
    }

    /**
     * Permissions of this tree.
     *
     * @return The permissions for this tree.
     */
    @NotNull
    private TreePermission getTreePermission() {
        if (treePermission == null
                || rootPermission != rootBuilder.treePermission) {
            NodeState base = builder.getBaseState();
            if (parent == null) {
                Tree baseTree = TreeFactory.createReadOnlyTree(base);
                treePermission = permissionProvider.get().getTreePermission(baseTree, TreePermission.EMPTY);
                rootPermission = treePermission;
            } else {
                treePermission = parent.getTreePermission().getChildPermission(name, base);
                rootPermission = parent.rootPermission;
            }
        }
        return treePermission;
    }

    private static boolean isType(@Nullable PropertyState property, Type<?> type) {
        Type<?> t = (property == null) ? null : property.getType();
        return t == type;
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Predicate for testing whether a given property is readable.
     */
    private class ReadablePropertyPredicate implements Predicate<PropertyState> {
        @Override
        public boolean test(@Nullable PropertyState property) {
            if (property != null) {
                String propertyName = property.getName();
                return NodeStateUtils.isHidden(propertyName) ||
                        getTreePermission().canRead(property) ||
                        isNew(propertyName);
            } else {
                return false;
            }
        }
    }

}
