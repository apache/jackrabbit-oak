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

import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * Abstract node store base class with in-memory node state builder
 * functionality.
 */
public abstract class MemoryNodeStore extends AbstractNodeStore {

    @Override
    public NodeStateBuilder getBuilder(NodeState base) {
        return new MemoryNodeStateBuilder(base);
    }

    @Override
    public void compare(NodeState before, NodeState after, NodeStateDiff diff) {
        if (after instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) after;
            if (before.equals(modified.getBase())) {
                modified.diffAgainstBase(diff);
            } else {
                super.compare(before, after, diff);
            }
        } else {
            super.compare(before, after, diff);
        }
    }

}
