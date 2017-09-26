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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

/**
 * Abstract base class for {@link DocumentRootBuilder} and
 * {@link DocumentNodeBuilder}.
 */
abstract class AbstractDocumentNodeBuilder extends MemoryNodeBuilder {

    public AbstractDocumentNodeBuilder(@Nonnull NodeState base) {
        super(base);
    }

    protected AbstractDocumentNodeBuilder(MemoryNodeBuilder parent,
                                          String name) {
        super(parent, name);
    }

    /**
     * Sets the named subtree to the given state. To avoid running out
     * of memory with large change-sets, the implementation recursively
     * copies all properties and child nodes to this builder so that the
     * purge mechanism has a chance to periodically flush partial changes
     * to the underlying storage database.
     *
     * See also: OAK-1768
     */
    @Override
    @Nonnull
    public NodeBuilder setChildNode(@Nonnull String name, @Nonnull NodeState state) {
        NodeBuilder builder = super.setChildNode(name, EMPTY_NODE);
        for (PropertyState property : state.getProperties()) {
            builder.setProperty(property);
        }
        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            builder.setChildNode(child.getName(), child.getNodeState());
        }
        return builder;
    }

    @Override
    protected abstract DocumentNodeBuilder createChildBuilder(String name);

    @Override
    @Nonnull
    public DocumentNodeBuilder getChildNode(@Nonnull String name) {
        checkValidName(name);
        return createChildBuilder(name);
    }
}
