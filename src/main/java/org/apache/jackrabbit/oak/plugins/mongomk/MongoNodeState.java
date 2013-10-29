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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.Collections;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link NodeState} implementation for the {@link MongoNodeStore}.
 */
class MongoNodeState extends AbstractNodeState {

    private final MongoNodeStore store;

    private final String path;

    private final Revision revision;

    MongoNodeState(@Nonnull MongoNodeStore store,
                   @Nonnull String path,
                   @Nonnull Revision revision) {
        this.store = checkNotNull(store);
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
    }

    String getPath() {
        return path;
    }

    Revision getRevision() {
        return revision;
    }

    //--------------------------< NodeState >-----------------------------------


    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof MongoNodeState) {
            MongoNodeState other = (MongoNodeState) that;
            if (revision.equals(other.revision) && path.equals(other.path)) {
                return true;
            } else {
                // TODO: optimize equals check for this case
            }
        }
        if (that instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) that);
        } else {
            return false;
        }
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        // TODO: implement
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        // TODO: implement
        return EmptyNodeState.MISSING_NODE;
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        // TODO: implement
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        // TODO: implement
        return new MemoryNodeBuilder(this);
    }

    //------------------------------< internal >--------------------------------

}
