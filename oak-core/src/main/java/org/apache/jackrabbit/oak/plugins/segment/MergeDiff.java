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
package org.apache.jackrabbit.oak.plugins.segment;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class MergeDiff implements NodeStateDiff {

    private final NodeBuilder builder;

    public MergeDiff(NodeBuilder builder) {
        this.builder = builder;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (!builder.hasProperty(after.getName())) {
            builder.setProperty(after);
        }
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        PropertyState other = builder.getProperty(before.getName());
        if (other != null && other.equals(before)) {
            builder.setProperty(after);
        }
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        PropertyState other = builder.getProperty(before.getName());
        if (other != null && other.equals(before)) {
            builder.removeProperty(before.getName());
        }
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (!builder.hasChildNode(name)) {
            builder.setChildNode(name, after);
        }
        return true;
    }

    @Override
    public boolean childNodeChanged(
            String name, NodeState before, NodeState after) {
        if (builder.hasChildNode(name)) {
            after.compareAgainstBaseState(
                    before, new MergeDiff(builder.child(name)));
        }
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (builder.hasChildNode(name)
                && before.equals(builder.child(name).getNodeState())) {
            builder.getChildNode(name).remove();
        }
        return true;
    }

}
