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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Basic in-memory node state implementation.
 */
public class MemoryNodeState extends AbstractNodeState
        implements Iterable<ChildNodeEntry> {

    /**
     * Singleton instance of an empty node state, i.e. one with neither
     * properties nor child nodes.
     */
    public static final NodeState EMPTY_NODE = new MemoryNodeState(
            Collections.<String, PropertyState>emptyMap(),
            Collections.<String, NodeState>emptyMap());

    private final Map<String, PropertyState> properties;

    private final Map<String, NodeState> nodes;

    /**
     * Creates a new node state with the given properties and child nodes.
     * The given maps are stored as references, so their contents and
     * iteration order must remain unmodified at least for as long as this
     * node state instance is in use.
     *
     * @param properties properties
     * @param nodes child nodes
     */
    public MemoryNodeState(
            Map<String, PropertyState> properties,
            Map<String, NodeState> nodes) {
        assert Collections.disjoint(properties.keySet(), nodes.keySet());
        this.properties = properties;
        this.nodes = nodes;
    }

    @Override
    public PropertyState getProperty(String name) {
        return properties.get(name);
    }

    @Override
    public long getPropertyCount() {
        return properties.size();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public NodeState getChildNode(String name) {
        return nodes.get(name);
    }

    @Override
    public long getChildNodeCount() {
        return nodes.size();
    }

    @Override
    public Iterable<ChildNodeEntry> getChildNodeEntries() {
        return this;
    }

    @Override
    public Iterator<ChildNodeEntry> iterator() {
        final Iterator<Map.Entry<String, NodeState>> iterator =
                nodes.entrySet().iterator();
        return new Iterator<ChildNodeEntry>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            @Override
            public ChildNodeEntry next() {
                return new MemoryChildNodeEntry(iterator.next());
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
