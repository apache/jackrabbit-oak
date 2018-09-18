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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LazyChildrenNodeState implements NodeState{
    private final NodeState delegate;
    private final ChildNodeStateProvider childProvider;

    LazyChildrenNodeState(NodeState delegate, ChildNodeStateProvider childProvider) {
        this.delegate = delegate;
        this.childProvider = childProvider;
    }

    @Override
    public boolean exists() {
        return delegate.exists();
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return delegate.hasProperty(name);
    }

    @Nullable
    @Override
    public PropertyState getProperty(@NotNull String name) {
        return delegate.getProperty(name);
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        return delegate.getBoolean(name);
    }

    @Override
    public long getLong(String name) {
        return delegate.getLong(name);
    }

    @Nullable
    @Override
    public String getString(String name) {
        return delegate.getString(name);
    }

    @NotNull
    @Override
    public Iterable<String> getStrings(@NotNull String name) {
        return delegate.getStrings(name);
    }

    @Nullable
    @Override
    public String getName(@NotNull String name) {
        return delegate.getName(name);
    }

    @NotNull
    @Override
    public Iterable<String> getNames(@NotNull String name) {
        return delegate.getNames(name);
    }

    @Override
    public long getPropertyCount() {
        return delegate.getPropertyCount();
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return delegate.getProperties();
    }

    @NotNull
    @Override
    public NodeBuilder builder() {
        return delegate.builder();
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        return AbstractNodeState.compareAgainstBaseState(this, base, diff);
    }

    //~-------------------------------< child node access >

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return childProvider.hasChildNode(name);
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        return childProvider.getChildNode(name);
    }

    @Override
    public long getChildNodeCount(long max) {
        return childProvider.getChildNodeCount(max);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return childProvider.getChildNodeNames();
    }

    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return childProvider.getChildNodeEntries();
    }
}
