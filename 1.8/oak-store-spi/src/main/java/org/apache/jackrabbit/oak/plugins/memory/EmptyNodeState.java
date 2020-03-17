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
package org.apache.jackrabbit.oak.plugins.memory;

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Singleton instances of empty and non-existent node states, i.e. ones
 * with neither properties nor child nodes.
 */
public final class EmptyNodeState implements NodeState {

    public static final NodeState EMPTY_NODE = new EmptyNodeState(true);

    public static final NodeState MISSING_NODE = new EmptyNodeState(false);

    private final boolean exists;

    private EmptyNodeState(boolean exists) {
        this.exists = exists;
    }

    @Override
    public boolean exists() {
        return exists;
    }

    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return false;
    }

    @Override @Nullable
    public PropertyState getProperty(@NotNull String name) {
        return null;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        return false;
    }

    @Override
    public long getLong(String name) {
        return 0;
    }

    @Override
    public String getString(String name) {
        return null;
    }

    @NotNull
    @Override
    public Iterable<String> getStrings(@NotNull String name) {
        return emptyList();
    }

    @Override @Nullable
    public String getName(@NotNull String name) {
        return null;
    }

    @Override @NotNull
    public Iterable<String> getNames(@NotNull String name) {
        return emptyList();
    }

    @Override @NotNull
    public Iterable<? extends PropertyState> getProperties() {
        return emptyList();
    }

    @Override
    public long getChildNodeCount(long max) {
        return 0;
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return false;
    }

    @Override @NotNull
    public NodeState getChildNode(@NotNull String name) {
        checkValidName(name);
        return MISSING_NODE;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return emptyList();
    }

    @Override @NotNull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return emptyList();
    }

    @Override @NotNull
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base != EMPTY_NODE && base.exists()) {
            for (PropertyState before : base.getProperties()) {
                if (!diff.propertyDeleted(before)) {
                    return false;
                }
            }
            for (ChildNodeEntry before : base.getChildNodeEntries()) {
                if (!diff.childNodeDeleted(
                        before.getName(), before.getNodeState())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean compareAgainstEmptyState(
            NodeState state, NodeStateDiff diff) {
        if (state != EMPTY_NODE && state.exists()) {
            for (PropertyState after : state.getProperties()) {
                if (!diff.propertyAdded(after)) {
                    return false;
                }
            }
            for (ChildNodeEntry after : state.getChildNodeEntries()) {
                if (!diff.childNodeAdded(after.getName(), after.getNodeState())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isEmptyState(NodeState state) {
        return state == EMPTY_NODE || state == MISSING_NODE;
    }

    //------------------------------------------------------------< Object >--

    public String toString() {
        if (exists) {
            return "{ }";
        } else {
            return "{N/A}";
        }
    }

    public boolean equals(Object object) {
        if (object == EMPTY_NODE || object == MISSING_NODE) {
            return exists == (object == EMPTY_NODE);
        } else if (object instanceof NodeState) {
            NodeState that = (NodeState) object;
            return that.getPropertyCount() == 0
                    && that.getChildNodeCount(1) == 0
                    && (exists == that.exists());
        } else {
            return false;
        }
    }

    public int hashCode() {
        return 0;
    }

}
