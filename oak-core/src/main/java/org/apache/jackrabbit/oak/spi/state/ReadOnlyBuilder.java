/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

/**
 * A node builder that throws an {@link UnsupportedOperationException} on
 * all attempts to modify the given base state.
 */
public class ReadOnlyBuilder implements NodeBuilder {

    private final NodeState state;

    public ReadOnlyBuilder(NodeState state) {
        this.state = state;
    }

    protected RuntimeException unsupported() {
        return new UnsupportedOperationException("This builder is read-only.");
    }

    @Override
    public boolean exists() {
        return state.exists();
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public NodeState getNodeState() {
        return state;
    }

    @Override
    public NodeState getBaseState() {
        return state;
    }

    @Override
    public void reset(NodeState state) {
        throw unsupported();
    }

    @Override
    public long getChildNodeCount() {
        return state.getChildNodeCount();
    }

    @Override
    public boolean hasChildNode(String name) {
        return state.hasChildNode(name);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return state.getChildNodeNames();
    }

    @Override @Nonnull
    public NodeBuilder setChildNode(String name, NodeState nodeState) {
        throw unsupported();
    }

    @Override @Nonnull
    public NodeBuilder removeChildNode(String name) {
        throw unsupported();
    }

    @Override
    public long getPropertyCount() {
        return state.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return state.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return state.hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return state.getProperty(name);
    }

    @Override
    public boolean getBoolean(String name) {
        return state.getBoolean(name);
    }

    @Override @CheckForNull
    public String getName(@Nonnull String name) {
        return state.getName(name);
    }

    @Override @Nonnull
    public Iterable<String> getNames(@Nonnull String name) {
        return state.getNames(name);
    }

    @Override @Nonnull
    public NodeBuilder removeProperty(String name) {
        throw unsupported();
    }

    @Override
    public NodeBuilder setProperty(PropertyState property) {
        throw unsupported();
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value) {
        throw unsupported();
    }

    @Override
    public <T> NodeBuilder setProperty(String name, T value, Type<T> type) {
        throw unsupported();
    }

    @Override
    public ReadOnlyBuilder child(String name) {
        NodeState child = state.getChildNode(name);
        if (child.exists()) {
            return new ReadOnlyBuilder(child);
        } else {
            throw unsupported();
        }
    }

    @Override @Nonnull
    public NodeBuilder getChildNode(@Nonnull String name) {
        return new ReadOnlyBuilder(state.getChildNode(name));
    }

    @Override @Nonnull
    public NodeBuilder setChildNode(@Nonnull String name) {
        throw unsupported();
    }

}
