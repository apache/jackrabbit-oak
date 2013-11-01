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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.kernel.FastCopyMove;
import org.apache.jackrabbit.oak.kernel.KernelNodeBuilder;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.LazyValue;

class SecureNodeBuilder implements NodeBuilder, FastCopyMove {

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
     * Access control context for evaluating access rights to the underlying raw builder
     */
    private final Context acContext;

    /**
     * Underlying node builder.
     */
    private final NodeBuilder builder;

    /**
     * Security context of this subtree, updated whenever the base revision
     * changes.
     */
    private SecurityContext securityContext = null; // initialized lazily

    /**
     * Possibly outdated reference to the security context of the root
     * builder. Used to detect when the base state (and thus the security
     * context) of the root builder has changed, and trigger an update of
     * the security context of this builder.
     *
     * @see #securityContext
     */
    private SecurityContext rootContext = null; // initialized lazily

    SecureNodeBuilder(
            @Nonnull NodeBuilder builder,
            @Nonnull LazyValue<PermissionProvider> permissionProvider,
            @Nonnull Context acContext) {
        this.rootBuilder = this;
        this.parent = null;
        this.name = null;
        this.permissionProvider = checkNotNull(permissionProvider);
        this.acContext = checkNotNull(acContext);
        this.builder = checkNotNull(builder);
    }

    private SecureNodeBuilder(SecureNodeBuilder parent, String name) {
        this.rootBuilder = parent.rootBuilder;
        this.parent = parent;
        this.name = name;
        this.permissionProvider = parent.permissionProvider;
        this.acContext = parent.acContext;
        this.builder = parent.builder.getChildNode(name);
    }

    @Override @CheckForNull
    public NodeState getBaseState() {
        NodeState base = builder.getBaseState();
        if (base != null) { // TODO: should use a missing state instead of null
            base = new SecureNodeState(base, getSecurityContext());
        }
        return base;
    }

    @Override @Nonnull
    public NodeState getNodeState() {
        return new SecureNodeState(builder.getNodeState(), getSecurityContext());
    }

    @Override
    public boolean exists() {
        return builder.exists() && getSecurityContext().canReadThisNode(); // TODO: isNew()?
    }

    @Override
    public boolean isNew() {
        return builder.isNew(); // TODO: might disclose hidden content
    }

    @Override
    public boolean isModified() {
        return builder.isModified();
    }

    public void baseChanged() {
        checkState(parent == null);
        securityContext = null; // trigger re-evaluation of the context
        rootContext = null;
        getSecurityContext();   // sets both securityContext and rootContext
    }

    @Override
    public boolean remove() {
        return exists() && builder.remove();
    }


    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) {
        return exists() && builder.moveTo(newParent, newName);
    }

    @Override
    public boolean copyTo(NodeBuilder newParent, String newName) {
        return exists() && builder.copyTo(newParent, newName);
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        PropertyState property = builder.getProperty(name);
        if (property != null && getSecurityContext().canReadProperty(property)) {
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
        if (getSecurityContext().canReadAll()) {
            return builder.getPropertyCount();
        } else {
            return size(filter(
                    builder.getProperties(),
                    new ReadablePropertyPredicate()));
        }
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        if (getSecurityContext().canReadAll()) {
            return builder.getProperties();
        } else if (getSecurityContext().canReadThisNode()) { // TODO: check DENY_PROPERTIES?
            return filter(
                    builder.getProperties(),
                    new ReadablePropertyPredicate());
        } else {
            return emptyList();
        }
    }

    @Override
    public boolean getBoolean(String name) {
        PropertyState property = getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    @Override @CheckForNull
    public String getName(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @Override @Nonnull
    public Iterable<String> getNames(@Nonnull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    @Override @Nonnull
    public NodeBuilder setProperty(@Nonnull PropertyState property) {
        builder.setProperty(property);
        return this;
    }

    @Override @Nonnull
    public <T> NodeBuilder setProperty(String name, @Nonnull T value) {
        builder.setProperty(name, value);
        return this;
    }

    @Override @Nonnull
    public <T> NodeBuilder setProperty(
            String name, @Nonnull T value, Type<T> type) {
        builder.setProperty(name, value, type);
        return this;
    }

    @Override @Nonnull
    public NodeBuilder removeProperty(String name) {
        if (hasProperty(name)) { // only remove properties that we can see
            builder.removeProperty(name);
        }
        return this;
    }

    @Override @Nonnull
    public Iterable<String> getChildNodeNames() {
        return filter(
                builder.getChildNodeNames(),
                new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String input) {
                        return input != null && getChildNode(input).exists();
                    }
                });
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return getChildNode(name).exists();
    }

    @Override @Nonnull
    public NodeBuilder child(@Nonnull String name) {
        if (hasChildNode(name)) {
            return getChildNode(name);
        } else {
            return setChildNode(name);
        }
    }

    @Override @Nonnull
    public NodeBuilder setChildNode(@Nonnull String name) {
        builder.setChildNode(name);
        return new SecureNodeBuilder(this, name);
    }

    @Override @Nonnull
    public NodeBuilder setChildNode(String name, @Nonnull NodeState nodeState) {
        builder.setChildNode(name, nodeState);
        return new SecureNodeBuilder(this, name);
    }

    @Override
    public NodeBuilder getChildNode(@Nonnull String name) {
        return new SecureNodeBuilder(this, checkNotNull(name));
    }

    @Override
    public synchronized long getChildNodeCount(long max) {
        if (getSecurityContext().canReadAll()) {
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
     * This implementation simply delegates back to {@code moveTo} method
     * of {@code source} passing the underlying builder for {@code newParent}.
     * @param source  source to move to this builder
     * @param newName  the new name
     * @return
     */
    @Override
    public boolean moveFrom(KernelNodeBuilder source, String newName) {
        return source.moveTo(builder, newName);
    }

    /**
     * This implementation simply delegates back to {@code copyTo} method
     * of {@code source} passing the underlying builder for {@code newParent}.
     * @param source  source to move to this builder
     * @param newName  the new name
     * @return
     */
    @Override
    public boolean copyFrom(KernelNodeBuilder source, String newName) {
        return source.copyTo(builder, newName);
    }

    /**
     * Security context of this subtree.
     */
    private SecurityContext getSecurityContext() {
        if (securityContext == null
                || rootContext != rootBuilder.securityContext) {
            NodeState base = builder.getBaseState();
            if (parent == null) {
                securityContext = new SecurityContext(
                        base, permissionProvider.get(), acContext);
                rootContext = securityContext;
            } else {
                SecurityContext parentContext = parent.getSecurityContext();
                securityContext = parentContext.getChildContext(name, base);
                rootContext = parent.rootContext;
            }
        }
        return securityContext;
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Predicate for testing whether a given property is readable.
     */
    private class ReadablePropertyPredicate implements Predicate<PropertyState> {
        @Override
        public boolean apply(@Nonnull PropertyState property) {
            return getSecurityContext().canReadProperty(property);
        }
    }

}
