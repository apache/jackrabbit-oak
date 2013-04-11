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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This implementation of {@code RebaseDiff} implements a
 * {@link org.apache.jackrabbit.oak.spi.state.NodeStateDiff},
 * which handled conflicts by giving precedence to our changes.
 * I.e. the changes in the {@code after} node state.
 */
class OurChangesRebaseDiff extends AbstractRebaseDiff {
    private OurChangesRebaseDiff(NodeBuilder builder) {
        super(builder);
    }

    public static NodeState rebase(NodeState before, NodeState after, NodeBuilder builder) {
        after.compareAgainstBaseState(before, new OurChangesRebaseDiff(builder));
        return builder.getNodeState();
    }

    @Override
    protected OurChangesRebaseDiff createDiff(NodeBuilder builder, String name) {
        return new OurChangesRebaseDiff(builder.child(name));
    }

    @Override
    protected void addExistingProperty(NodeBuilder builder, PropertyState after) {
        builder.setProperty(after);
    }

    @Override
    protected void changeDeletedProperty(NodeBuilder builder, PropertyState after) {
        builder.setProperty(after);
    }

    @Override
    protected void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        builder.setProperty(after);
    }

    @Override
    protected void deleteDeletedProperty(NodeBuilder builder, PropertyState before) {
        // ignore
    }

    @Override
    protected void deleteChangedProperty(NodeBuilder builder, PropertyState before) {
        builder.removeProperty(before.getName());
    }

    @Override
    protected void addExistingNode(NodeBuilder builder, String name, NodeState after) {
        builder.setNode(name, after);
    }

    @Override
    protected void changeDeletedNode(NodeBuilder builder, String name, NodeState after) {
        builder.setNode(name, after);
    }

    @Override
    protected void deleteDeletedNode(NodeBuilder builder, String name, NodeState before) {
        // ignore
    }

    @Override
    protected void deleteChangedNode(NodeBuilder builder, String name, NodeState before) {
        builder.removeNode(name);
    }

}
