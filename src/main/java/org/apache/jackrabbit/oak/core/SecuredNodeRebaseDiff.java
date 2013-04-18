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
 * {@link org.apache.jackrabbit.oak.spi.state.NodeStateDiff}
 * for applying changes made on top of secure node states
 * to a node builder for the underlying non secure node state
 * of the before state. That is, the only expected conflicts
 * are adding an existing property and adding an existing node.
 * These conflicts correspond to the shadowing of hidden properties
 * and nodes in transient space, respectively.
 *
 * @see SecureNodeState
 */
class SecuredNodeRebaseDiff extends AbstractRebaseDiff {
    private SecuredNodeRebaseDiff(NodeBuilder builder) {
        super(builder);
    }

    /**
     * Rebase the differences between {@code before} and {@code after} on top of
     * {@code builder}. Add existing node and add existing property conflicts give
     * precedence to the {@code after} state. All other conflicts are unexpected
     * and result in an {@code IllegalStateException}.
     *
     * @param before   before state
     * @param after    after state
     * @param builder  builder based on the before state
     * @return  node state resulting from applying the differences between
     *          {@code before} and {@code after} to {@code builder}
     * @throws IllegalStateException  if an unexpected conflict occurs due to
     *         {@code builder} not being based on {@code before}.
     */
    public static NodeState rebase(NodeState before, NodeState after, NodeBuilder builder) {
        after.compareAgainstBaseState(before, new SecuredNodeRebaseDiff(builder));
        return builder.getNodeState();
    }

    @Override
    protected SecuredNodeRebaseDiff createDiff(NodeBuilder builder, String name) {
        return new SecuredNodeRebaseDiff(builder.child(name));
    }

    @Override
    protected void addExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        builder.setProperty(after);
    }

    @Override
    protected void changeDeletedProperty(NodeBuilder builder, PropertyState after) {
        throw new IllegalStateException("Unexpected conflict: change deleted property: " + after);
    }

    @Override
    protected void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        throw new IllegalStateException("Unexpected conflict: change changed property from " +
                before + " to " + after);
    }

    @Override
    protected void deleteDeletedProperty(NodeBuilder builder, PropertyState before) {
        throw new IllegalStateException("Unexpected conflict: delete deleted property: " + before);
    }

    @Override
    protected void deleteChangedProperty(NodeBuilder builder, PropertyState before) {
        throw new IllegalStateException("Unexpected conflict: delete changed property: " + before);
    }

    @Override
    protected void addExistingNode(NodeBuilder builder, String name, NodeState before, NodeState after) {
        // FIXME (OAK-709) after might be a secured node instead of the underlying non secured node.
        // Pushing this on the non secured builder is wrong.
        // AFAICS this is only relevant when the after node state has been moved here
        builder.setNode(name, after);
    }

    @Override
    protected void changeDeletedNode(NodeBuilder builder, String name, NodeState after) {
        throw new IllegalStateException("Unexpected conflict: change deleted node: " +
                name + " : " + after);
    }

    @Override
    protected void deleteDeletedNode(NodeBuilder builder, String name, NodeState before) {
        throw new IllegalStateException("Unexpected conflict: delete deleted node: " +
                name + " : " + before);
    }

    @Override
    protected void deleteChangedNode(NodeBuilder builder, String name, NodeState before) {
        // FIXME Should never be called. OAK-781 should fix this.
//        throw new IllegalStateException("Unexpected conflict: delete changed node: " +
//                name + " : " + before);
    }

}
