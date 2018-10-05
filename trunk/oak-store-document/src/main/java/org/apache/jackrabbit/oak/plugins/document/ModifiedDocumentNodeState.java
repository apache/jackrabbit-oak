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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * A node state based on a {@link DocumentNodeState} with some modifications
 * applied on top of it represented by {@link #modified}. This node state is
 * a thin wrapper around {@link #modified} and creates a new {@link NodeBuilder}
 * connected to the {@link #branch} on {@link #builder()}.
 */
class ModifiedDocumentNodeState extends AbstractNodeState {

    private final DocumentNodeStore store;

    private final DocumentNodeStoreBranch branch;

    private final DocumentNodeState base;

    private final NodeState modified;

    ModifiedDocumentNodeState(@NotNull DocumentNodeStore store,
                              @NotNull DocumentNodeStoreBranch branch,
                              @NotNull DocumentNodeState base,
                              @NotNull NodeState modified) {
        this.store = checkNotNull(store);
        this.branch = checkNotNull(branch);
        this.base = checkNotNull(base);
        this.modified = checkNotNull(modified);
    }

    @Override
    public boolean exists() {
        return modified.exists();
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return modified.getProperties();
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return modified.hasChildNode(name);
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name)
            throws IllegalArgumentException {
        return modified.getChildNode(name);
    }

    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return modified.getChildNodeEntries();
    }

    @NotNull
    @Override
    public NodeBuilder builder() {
        NodeBuilder builder = new DocumentRootBuilder(base, store, branch);
        modified.compareAgainstBaseState(base, new ApplyDiff(builder));
        return builder;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof AbstractDocumentNodeState) {
            AbstractDocumentNodeState other = (AbstractDocumentNodeState) that;
            if (!base.getPath().equals(other.getPath())) {
                // path does not match: not equals
                // (even if the properties are equal)
                return false;
            }
            if (revisionEquals(base, other)) {
                // other is equal to our base state
                // perform an equals diff with base and modified
                return EqualsDiff.equals(base, modified);
            }
            // revision does not match: might still be equals
        } else if (that instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) that;
            if (modified.getBaseState() == base) {
                // base states are the same, compare the modified
                return EqualsDiff.equals(this.modified, modified);
            }
        }
        if (that instanceof NodeState) {
            return AbstractNodeState.equals(modified, (NodeState) that);
        }
        return false;
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base) {
            return true;
        } else if (base == EMPTY_NODE || !base.exists()) {
            // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (this.base == base) {
            return modified.compareAgainstBaseState(this.base, diff);
        } else if (base instanceof AbstractDocumentNodeState) {
            AbstractDocumentNodeState other = (AbstractDocumentNodeState) base;
            if (this.base.getPath().equals(other.getPath())) {
                if (revisionEquals(this.base, other)) {
                    return modified.compareAgainstBaseState(this.base, diff);
                }
            }
        }
        // fall back to the generic node state diff algorithm
        return super.compareAgainstBaseState(base, diff);
    }

    private boolean revisionEquals(AbstractDocumentNodeState a,
                                   AbstractDocumentNodeState b) {
        RevisionVector rv1 = a.getLastRevision();
        rv1 = rv1 != null ? rv1.asTrunkRevision() : null;
        RevisionVector rv2 = b.getLastRevision();
        rv2 = rv2 != null ? rv2.asTrunkRevision() : null;
        return rv1 != null && rv1.equals(rv2);
    }
}
