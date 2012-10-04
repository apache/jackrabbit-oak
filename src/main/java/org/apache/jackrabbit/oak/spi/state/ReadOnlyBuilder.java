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

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;

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
    public NodeState getNodeState() {
        return state;
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
    public NodeBuilder setNode(String name, NodeState nodeState) {
        throw unsupported();
    }

    @Override @Nonnull
    public NodeBuilder removeNode(String name) {
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
    public PropertyState getProperty(String name) {
        return state.getProperty(name);
    }

    @Override @Nonnull
    public NodeBuilder setProperty(String name, CoreValue value) {
        throw unsupported();
    }

    @Override @Nonnull
    public NodeBuilder setProperty(String name, List<CoreValue> values) {
        throw unsupported();
    }

    @Override @Nonnull
    public NodeBuilder set(String name, String value) {
        throw unsupported();
    }

    @Override @Nonnull
    public NodeBuilder set(String name, String... value) {
        throw unsupported();
    }

    @Override @Nonnull
    public NodeBuilder removeProperty(String name) {
        throw unsupported();
    }

    @Override
    public ReadOnlyBuilder getChildBuilder(String name) {
        NodeState child = state.getChildNode(name);
        if (child != null) {
            return new ReadOnlyBuilder(child);
        } else {
            throw unsupported();
        }
    }

}
