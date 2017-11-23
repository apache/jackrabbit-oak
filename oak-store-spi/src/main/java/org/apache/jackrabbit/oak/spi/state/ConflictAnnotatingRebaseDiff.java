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

import org.apache.jackrabbit.oak.api.PropertyState;

import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_CHANGED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_CHANGED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.ADD_EXISTING_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.CHANGE_DELETED_NODE;
import static org.apache.jackrabbit.oak.spi.state.ConflictType.DELETE_DELETED_NODE;

/**
 * This implementation of {@code AbstractRebaseDiff} implements a {@link NodeStateDiff},
 * which performs the conflict handling as defined in {@link NodeStore#rebase(NodeBuilder)}
 * on the Oak SPI state level by annotating conflicting items with conflict
 * markers.
 */
public class ConflictAnnotatingRebaseDiff extends AbstractRebaseDiff {
    public static final String CONFLICT = ":conflict";
    public static final String BASE = ":base";
    public static final String OURS = ":ours";

    public ConflictAnnotatingRebaseDiff(NodeBuilder builder) {
        super(builder);
    }

    @Override
    protected ConflictAnnotatingRebaseDiff createDiff(NodeBuilder builder, String name) {
        return new ConflictAnnotatingRebaseDiff(builder.child(name));
    }

    @Override
    protected void addExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        NodeBuilder cb = conflictMarker(builder, ADD_EXISTING_PROPERTY);
        cb.child(BASE).setProperty(before);
        cb.child(OURS).setProperty(after);
    }

    @Override
    protected void changeDeletedProperty(NodeBuilder builder, PropertyState after, PropertyState base) {
        NodeBuilder cb = conflictMarker(builder, CHANGE_DELETED_PROPERTY);
        cb.child(BASE).setProperty(base);
        cb.child(OURS).setProperty(after);
    }

    @Override
    protected void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
        NodeBuilder cb = conflictMarker(builder, CHANGE_CHANGED_PROPERTY);
        cb.child(BASE).setProperty(before);
        cb.child(OURS).setProperty(after);
    }

    @Override
    protected void deleteDeletedProperty(NodeBuilder builder, PropertyState before) {
        NodeBuilder cb = conflictMarker(builder, DELETE_DELETED_PROPERTY);
        cb.child(BASE).setProperty(before);
    }

    @Override
    protected void deleteChangedProperty(NodeBuilder builder, PropertyState before) {
        NodeBuilder cb = conflictMarker(builder, DELETE_CHANGED_PROPERTY);
        cb.child(BASE).setProperty(before);
    }

    @Override
    protected void addExistingNode(NodeBuilder builder, String name, NodeState before, NodeState after) {
        NodeBuilder cb = conflictMarker(builder, ADD_EXISTING_NODE);
        cb.child(BASE).setChildNode(name, before);
        cb.child(OURS).setChildNode(name, after);
    }

    @Override
    protected void changeDeletedNode(NodeBuilder builder, String name, NodeState after, NodeState base) {
        NodeBuilder cb = conflictMarker(builder, CHANGE_DELETED_NODE);
        cb.child(BASE).setChildNode(name, base);
        cb.child(OURS).setChildNode(name, after);
    }

    @Override
    protected void deleteDeletedNode(NodeBuilder builder, String name, NodeState before) {
        NodeBuilder cb = conflictMarker(builder, DELETE_DELETED_NODE);
        cb.child(BASE).setChildNode(name, before);
    }

    @Override
    protected void deleteChangedNode(NodeBuilder builder, String name, NodeState before) {
        NodeBuilder cb = conflictMarker(builder, DELETE_CHANGED_NODE);
        cb.child(BASE).setChildNode(name, before);
    }

    private static NodeBuilder conflictMarker(NodeBuilder builder, ConflictType ct) {
        return builder.child(CONFLICT).child(ct.getName());
    }

}
