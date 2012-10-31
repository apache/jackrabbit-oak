/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class CompositeNodeStateDiff implements NodeStateDiff {

    private final List<NodeStateDiff> providers;

    public CompositeNodeStateDiff(List<NodeStateDiff> providers) {
        this.providers = providers;
    }

    public CompositeNodeStateDiff(NodeStateDiff... providers) {
        this(Arrays.asList(providers));
    }

    @Override
    public void propertyAdded(PropertyState after) {
        for (NodeStateDiff d : providers) {
            d.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (NodeStateDiff d : providers) {
            d.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (NodeStateDiff d : providers) {
            d.propertyDeleted(before);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        for (NodeStateDiff d : providers) {
            d.childNodeAdded(name, after);
        }
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        for (NodeStateDiff d : providers) {
            d.childNodeChanged(name, before, after);
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        for (NodeStateDiff d : providers) {
            d.childNodeDeleted(name, before);
        }
    }
}
