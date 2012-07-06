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
import org.apache.jackrabbit.oak.kernel.PropertyStateImpl;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Basic in-memory node state builder.
 */
class MemoryNodeStateBuilder implements NodeStateBuilder {

    private final NodeState base;

    /**
     * Set of added, modified or removed ({@code null} value) property states.
     */
    private final Map<String, PropertyState> properties =
            new HashMap<String, PropertyState>();

    /**
     * Set of builders for added, modified or removed ({@code null} value)
     * child nodes.
     */
    private final Map<String, NodeStateBuilder> builders =
            new HashMap<String, NodeStateBuilder>();

    public MemoryNodeStateBuilder(NodeState base) {
        assert base != null;
        this.base = base;
    }

    @Override
    public NodeState getNodeState() {
        if (properties.isEmpty() && builders.isEmpty()) {
            return base; // shortcut
        } else {
            Map<String, NodeState> nodes = new HashMap<String, NodeState>();
            for (Map.Entry<String, NodeStateBuilder> entry
                    : builders.entrySet()) {
                NodeStateBuilder builder = entry.getValue();
                if (builder != null) {
                    nodes.put(entry.getKey(), builder.getNodeState());
                } else {
                    nodes.put(entry.getKey(), null);
                }
            }
            return new ModifiedNodeState(
                    base, snapshot(properties), snapshot(nodes));
        }
    }

    /**
     * Returns an optimized snapshot of the current state of the given map.
     *
     * @param map mutable map
     * @return optimized snapshot
     */
    private static <T> Map<String, T> snapshot(Map<String, T> map) {
        if (map.isEmpty()) {
            return Collections.emptyMap();
        } else if (map.size() == 1) {
            Map.Entry<String, T> entry = map.entrySet().iterator().next();
            return Collections.singletonMap(entry.getKey(), entry.getValue());
        } else {
            return new HashMap<String, T>(map);
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
        PropertyState property = new PropertyStateImpl(name, value);
        properties.put(name, property);
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        PropertyState property = new PropertyStateImpl(name, values);
        properties.put(name, property);
    }

    @Override
    public void removeProperty(String name) {
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
