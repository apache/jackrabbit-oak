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

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A node builder implementation for MongoMK.
 */
class MongoNodeBuilder extends MemoryNodeBuilder {
    private NodeState base;

    protected MongoNodeBuilder(MongoNodeState base) {
        super(base);
    }

    private MongoNodeBuilder(MongoNodeBuilder parent, String name) {
        super(parent, name);
    }

    @Override
    public NodeState getBaseState() {
        if (base == null) {
            base = getParent().getBaseState().getChildNode(getName());
        }
        return base;
    }

    @Override
    protected MongoNodeBuilder createChildBuilder(String name) {
        return new MongoNodeBuilder(this, name);
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) {
        if (newParent instanceof MongoNodeBuilder) {
            // check if this builder is an ancestor of newParent or newParent
            MongoNodeBuilder parent = (MongoNodeBuilder) newParent;
            while (parent != null) {
                if (parent == this) {
                    return false;
                }
                parent = (MongoNodeBuilder) parent.getParent();
            }
        }
        return super.moveTo(newParent, newName);
    }
}
