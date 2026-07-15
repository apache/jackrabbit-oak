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
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * A node state diff handler that applies all reported changes
 * as-is to the given node builder. No conflict detection or resolution
 * is attempted. The main use case for this class is to call all the
 * {@link NodeBuilder} methods necessary to go from a given base state
 * to any given target state.
 * <p>
 * The expected usage pattern looks like this:
 * <pre>
 * NodeState base = ...;
 * NodeState target = ...;
 * NodeBuilder builder = base.builder();
 * target.compareAgainstBaseState(base, new ApplyDiff(builder));
 * assertEquals(target, builder.getNodeState());
 * </pre>
 * <p>
 * Alternatively, the {@link #apply(NodeState)} method can be used to set
 * the content of a given builder:
 * <pre>
 * NodeBuilder builder = ...;
 * NodeState target = ...;
 * new ApplyDiff(builder).apply(target);
 * assertEquals(target, builder.getNodeState());
 * </pre>
 */
public class ApplyDiff implements NodeStateDiff {

    protected final NodeBuilder builder;

    public ApplyDiff(NodeBuilder builder) {
        this.builder = builder;
    }

    public void apply(NodeState target) {
        target.compareAgainstBaseState(builder.getNodeState(), this);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        builder.setProperty(after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        builder.setProperty(after);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        builder.removeProperty(before.getName());
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        builder.setChildNode(name, after);
        return true;
    }

    @Override
    public boolean childNodeChanged(
            String name, NodeState before, NodeState after) {
        return after.compareAgainstBaseState(
                before, new ApplyDiff(builder.getChildNode(name)));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        builder.getChildNode(name).remove();
        return true;
    }

}
