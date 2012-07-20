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

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Basic in-memory node state builder.
 */
public class MemoryNodeStateBuilder implements NodeStateBuilder {

    private final NodeState base;

    /**
     * Set of added, modified or removed ({@code null} value) property states.
     */
    private Map<String, PropertyState> properties =
            new HashMap<String, PropertyState>();

    /**
     * Set of builders for added, modified or removed ({@code null} value)
     * child nodes.
     */
    private final Map<String, NodeStateBuilder> builders =
            new HashMap<String, NodeStateBuilder>();

    /**
     * Flag to indicate that the current {@link #properties} map is being
     * referenced by a {@link ModifiedNodeState} instance returned by a
     * previous {@link #getNodeState()} call, and thus should not be
     * modified unless first explicitly {@link #unfreeze() unfrozen}.
     */
    private boolean frozen = false;

    public MemoryNodeStateBuilder(NodeState base) {
        assert base != null;
        this.base = base;
    }

    /**
     * Ensures that the current {@link #properties} map is not {@link #frozen}.
     */
    private void unfreeze() {
        if (frozen) {
            properties = new HashMap<String, PropertyState>(properties);
            frozen = false;
        }
    }

    @Override
    public NodeState getNodeState() {
        Map<String, PropertyState> props = Collections.emptyMap();
        if (!properties.isEmpty()) {
            frozen = true;
            props = properties;
        }

        Map<String, NodeState> nodes = Collections.emptyMap();
        if (!builders.isEmpty()) {
            nodes = new HashMap<String, NodeState>(builders.size() * 2);
            for (Map.Entry<String, NodeStateBuilder> entry
                    : builders.entrySet()) {
                NodeStateBuilder builder = entry.getValue();
                if (builder != null) {
                    nodes.put(entry.getKey(), builder.getNodeState());
                } else {
                    nodes.put(entry.getKey(), null);
                }
            }
        }

        if (props.isEmpty() && nodes.isEmpty()) {
            return base;
        } else {
            return new ModifiedNodeState(base, props, nodes);
        }
    }

    @Override
    public void setNode(String name, NodeState nodeState) {
        if (nodeState == null) {
            removeNode(name);
        } else {
            if (nodeState.equals(base.getChildNode(name))) {
                builders.remove(name);
            } else {
                builders.put(name, new MemoryNodeStateBuilder(nodeState));
            }
        }
    }

    @Override
    public void removeNode(String name) {
        if (base.getChildNode(name) != null) {
            builders.put(name, null);
        } else {
            builders.remove(name);
        }
    }

    @Override
    public void setProperty(String name, CoreValue value) {
        unfreeze();
        properties.put(name, new SinglePropertyState(name, value));
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        unfreeze();
        if (values.isEmpty()) {
            properties.put(name, new EmptyPropertyState(name));
        } else {
            properties.put(name, new MultiPropertyState(name, values));
        }
    }

    @Override
    public void removeProperty(String name) {
        unfreeze();
        if (base.getProperty(name) != null) {
            properties.put(name, null);
        } else {
            properties.remove(name);
        }
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        NodeStateBuilder builder = builders.get(name);
        if (builder == null) {
            NodeState baseState = base.getChildNode(name);
            if (baseState == null) {
                baseState = MemoryNodeState.EMPTY_NODE;
            }
            builder = new MemoryNodeStateBuilder(baseState);
            builders.put(name, builder);
        }
        return builder;
    }

}
