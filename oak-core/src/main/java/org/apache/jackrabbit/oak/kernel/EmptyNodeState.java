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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.Iterators;

import java.util.Iterator;

/**
 * This {@link NodeState} instance represents an empty node state.
 * That is, a node state which no child node entries and no properties.
 */
public class EmptyNodeState extends AbstractNodeState {
    public static final NodeState INSTANCE = new EmptyNodeState();

    private EmptyNodeState() {
        // empty
    }

    @Override
    public PropertyState getProperty(String name) {
        return null;
    }

    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                return Iterators.empty();
            }
        };
    }

    @Override
    public NodeState getChildNode(String name) {
        return null;
    }

    @Override
    public long getChildNodeCount() {
        return 0;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count) {
        return new Iterable<ChildNodeEntry>() {
            @Override
            public Iterator<ChildNodeEntry> iterator() {
                return Iterators.empty();
            }
        };
    }
}
