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
package org.apache.jackrabbit.oak.plugins.memory;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Basic JavaBean implementation of a child node entry.
 */
public class MemoryChildNodeEntry extends AbstractChildNodeEntry {

    public static <E extends Entry<String, ? extends NodeState>> Iterable<ChildNodeEntry> iterable(
            Iterable<E> set) {
        return Iterables.transform(
                set,
                new Function<Entry<String, ? extends NodeState>, ChildNodeEntry>() {
                    @Override
                    public ChildNodeEntry apply(Entry<String, ? extends NodeState> entry) {
                        return new MemoryChildNodeEntry(entry.getKey(), entry.getValue());
                    }
                });
    }

    private final String name;

    private final NodeState node;

    /**
     * Creates a child node entry with the given name and referenced
     * child node state.
     *
     * @param name child node name
     * @param node child node state
     */
    public MemoryChildNodeEntry(String name, NodeState node) {
        this.name = checkNotNull(name);
        this.node = checkNotNull(node);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public NodeState getNodeState() {
        return node;
    }

}
