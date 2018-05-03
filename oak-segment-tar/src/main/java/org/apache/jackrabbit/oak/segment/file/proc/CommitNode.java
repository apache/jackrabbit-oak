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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Commit;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class CommitNode extends AbstractNode {

    private final Proc.Backend backend;

    private final String handle;

    CommitNode(Proc.Backend backend, String handle) {
        this.backend = backend;
        this.handle = handle;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return backend.getCommit(handle).map(this::getProperties).orElse(emptySet());
    }

    private Iterable<PropertyState> getProperties(Commit entry) {
        return Lists.newArrayList(
            PropertyStates.createProperty("timestamp", entry.getTimestamp(), Type.LONG),
            PropertyStates.createProperty("revision", entry.getRevision())
        );
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return backend.getCommit(handle)
            .flatMap(Proc.Backend.Commit::getRoot)
            .map(r -> singleton(new MemoryChildNodeEntry("root", r)))
            .orElse(emptySet());
    }

}
