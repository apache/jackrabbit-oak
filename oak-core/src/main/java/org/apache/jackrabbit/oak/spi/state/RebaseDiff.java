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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * {@code RebaseDiff} implements a {@link NodeStateDiff}, which performs
 * the conflict handling as defined in {@link MicroKernel#rebase(String, String)}
 * on the Oak SPI state level.
 * <p/>
 * Intended use of this class is to re-base a branched version of the node state
 * tree. Given below situation:
 * <pre>
 *     + head (master)
 *     |
 *     | + branch
 *     |/
 *     + base
 *     |
 * </pre>
 * The current state on the master branch is {@code head} and a branch
 * was created at {@code base}. The current state on the branch is
 * {@code branch}. Re-basing {@code branch} to the current
 * {@code head} works as follows:
 * <pre>
 *     NodeState head = ...
 *     NodeState branch = ...
 *     NodeState base = ...
 *     NodeBuilder builder = new MemoryNodeBuilder(head);
 *     branch.compareAgainstBaseState(base, new RebaseDiff(builder));
 *     branch = builder.getNodeState();
 * </pre>
 * The result is:
 * <pre>
 *       + branch
 *      /
 *     + head (master)
 *     |
 * </pre>
 * <p/>
 */
public class RebaseDiff implements NodeStateDiff {

    public static final String CONFLICT = ":conflict";
    public static final String DELETE_CHANGED_PROPERTY = "deleteChangedProperty";
    public static final String DELETE_CHANGED_NODE = "deleteChangedNode";
    public static final String ADD_EXISTING_PROPERTY = "addExistingProperty";
    public static final String CHANGE_DELETED_PROPERTY = "changeDeletedProperty";
    public static final String CHANGE_CHANGED_PROPERTY = "changeChangedProperty";
    public static final String DELETE_DELETED_PROPERTY = "deleteDeletedProperty";
    public static final String ADD_EXISTING_NODE = "addExistingNode";
    public static final String CHANGE_DELETED_NODE = "changeDeletedNode";
    public static final String DELETE_DELETED_NODE = "deleteDeletedNode";

    private final NodeBuilder builder;

    public RebaseDiff(NodeBuilder builder) {
        this.builder = builder;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        PropertyState other = builder.getProperty(after.getName());
        if (other == null) {
            builder.setProperty(after);
        } else if (!other.equals(after)) {
            conflictMarker(ADD_EXISTING_PROPERTY).setProperty(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        PropertyState other = builder.getProperty(before.getName());
        if (other == null) {
            conflictMarker(CHANGE_DELETED_PROPERTY).setProperty(after);
        } else if (other.equals(before)) {
            builder.setProperty(after);
        } else if (!other.equals(after)) {
            conflictMarker(CHANGE_CHANGED_PROPERTY).setProperty(after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        PropertyState other = builder.getProperty(before.getName());
        if (other == null) {
            conflictMarker(DELETE_DELETED_PROPERTY).setProperty(before);
        } else if (other.equals(before)) {
            builder.removeProperty(before.getName());
        } else {
            conflictMarker(DELETE_CHANGED_PROPERTY).setProperty(before);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        if (builder.hasChildNode(name)) {
            conflictMarker(ADD_EXISTING_NODE).setNode(name, after);
        } else {
            builder.setNode(name, after);
        }
    }

    @Override
    public void childNodeChanged(
            String name, NodeState before, NodeState after) {
        if (builder.hasChildNode(name)) {
            after.compareAgainstBaseState(
                    before, new RebaseDiff(builder.child(name)));
        } else {
            conflictMarker(CHANGE_DELETED_NODE).setNode(name, after);
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        if (!builder.hasChildNode(name)) {
            conflictMarker(DELETE_DELETED_NODE).setNode(name, before);
        } else if (before.equals(builder.child(name).getNodeState())) {
            builder.removeNode(name);
        } else {
            conflictMarker(DELETE_CHANGED_NODE).setNode(name, before);
        }
    }

    private NodeBuilder conflictMarker(String name) {
        return builder.child(CONFLICT).child(name);
    }

}
