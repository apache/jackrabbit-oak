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
 *
 */

package org.apache.jackrabbit.oak.segment.file.proc;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

class StoreNode extends AbstractNode {

    private final Backend backend;

    StoreNode(Backend backend) {
        this.backend = backend;
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return backend.tarExists(name);
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        if (backend.tarExists(name)) {
            return new TarNode(backend, name);
        }
        return EmptyNodeState.MISSING_NODE;
    }

    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        List<ChildNodeEntry> entries = new ArrayList<>();

        for (String name : backend.getTarNames()) {
            entries.add(new MemoryChildNodeEntry(name, new TarNode(backend, name)));
        }

        return entries;
    }

}
