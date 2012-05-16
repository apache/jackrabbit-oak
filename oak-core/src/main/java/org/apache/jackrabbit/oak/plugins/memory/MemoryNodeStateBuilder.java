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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.kernel.PropertyStateImpl;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

/**
 * Basic in-memory node state builder.
 */
public class MemoryNodeStateBuilder implements NodeStateBuilder {

    private final NodeState base;

    private final Map<String, PropertyState> properties =
            new HashMap<String, PropertyState>();

    private final Map<String, NodeState> nodes =
            new HashMap<String, NodeState>();

    public MemoryNodeStateBuilder(NodeState base) {
        assert base != null;
        this.base = base;
    }

    @Override
    public NodeState getNodeState() {
        return new ModifiedNodeState(
                base,
                new HashMap<String, PropertyState>(properties),
                new HashMap<String, NodeState>(nodes));
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NodeStateBuilder addNode(String name, NodeState nodeState) {
        nodes.put(name, nodeState);
        return this;
    }

    @Override
    public NodeStateBuilder addNode(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean removeNode(String name) {
        nodes.put(name, null);
        return false;
    }

    @Override
    public PropertyState setProperty(String name, CoreValue value) {
        PropertyState property = new PropertyStateImpl(name, value);
        properties.put(name, property);
        return property;
    }

    @Override
    public PropertyState setProperty(String name, List<CoreValue> values) {
        PropertyState property = new PropertyStateImpl(name, values);
        properties.put(name, property);
        return property;
    }

    @Override
    public void removeProperty(String name) {
        properties.put(name, null);
    }

    @Override
    public boolean moveTo(NodeStateBuilder destParent, String destName) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean copyTo(NodeStateBuilder destParent, String destName) {
        // TODO Auto-generated method stub
        return false;
    }

}
