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
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class SecureNodeBuilder implements NodeBuilder {

    /**
     * Underlying node builder.
     */
    private final NodeBuilder builder;

    /**
     * Security context of this subtree.
     */
    private final SecurityContext context;

    SecureNodeBuilder(
            @Nonnull NodeBuilder builder, @Nonnull SecurityContext context) {
        this.builder = checkNotNull(builder);
        this.context = checkNotNull(context);
    }

    @Override @CheckForNull
    public NodeState getBaseState() {
        NodeState base = builder.getBaseState();
        if (base != null) { // TODO: should use a missing state instead of null
            base = new SecureNodeState(base, context); // TODO: baseContext?
        }
        return base;
    }

    @Override @Nonnull
    public NodeState getNodeState() {
        return new SecureNodeState(builder.getNodeState(), context);
    }

    @Override
    public boolean exists() {
        return builder.exists() && context.canReadThisNode(); // TODO: isNew()?
    }

    @Override
    public boolean isNew() {
        return builder.isNew(); // TODO: might disclose hidden content
    }

    @Override
    public boolean isModified() {
        return builder.isModified();
    }

    @Override
    public void reset(@Nonnull NodeState state) throws IllegalStateException {
        builder.reset(state); // NOTE: can be dangerous with SecureNodeState
    }

    @Override
    public boolean remove() {
        return exists() && builder.remove();
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        PropertyState property = builder.getProperty(name);
        if (property != null && context.canReadProperty(property)) {
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
        if (context.canReadAll()) {
            return builder.getPropertyCount();
        } else {
            return size(filter(
                    builder.getProperties(),
                    new ReadablePropertyPredicate()));
        }
    }

    @Override @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        if (context.canReadAll()) {
            return builder.getProperties();
        } else if (context.canReadThisNode()) { // TODO: check DENY_PROPERTIES?
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
        NodeBuilder child = builder.setChildNode(name);
        return new SecureNodeBuilder(
                child, context.getChildContext(name, child.getBaseState()));
    }

    @Override @Nonnull
    public NodeBuilder setChildNode(String name, @Nonnull NodeState nodeState) {
        NodeBuilder child = builder.setChildNode(name, nodeState);
        return new SecureNodeBuilder(
                child, context.getChildContext(name, child.getBaseState()));
    }

    @Override
    public NodeBuilder getChildNode(@Nonnull String name) {
        NodeBuilder child = builder.getChildNode(checkNotNull(name));
        if (child.exists() && !context.canReadAll()) {
            return new SecureNodeBuilder(
                    child, context.getChildContext(name, child.getBaseState()));
        } else {
            return child;
        }
    }

    @Override
    public synchronized long getChildNodeCount() {
        if (context.canReadAll()) {
            return builder.getChildNodeCount();
        } else {
            return size(getChildNodeNames());
        }
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Predicate for testing whether a given property is readable.
     */
    private class ReadablePropertyPredicate implements Predicate<PropertyState> {
        @Override
        public boolean apply(@Nonnull PropertyState property) {
            return context.canReadProperty(property);
        }
    }

}
