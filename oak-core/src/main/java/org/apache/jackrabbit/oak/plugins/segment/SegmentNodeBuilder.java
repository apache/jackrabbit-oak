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
package org.apache.jackrabbit.oak.plugins.segment;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class SegmentNodeBuilder extends MemoryNodeBuilder {

    protected NodeState base;

    protected long baseRevision;

    protected SegmentNodeBuilder(SegmentNodeState base) {
        super(base);
        this.base = base;
        this.baseRevision = 0;
    }

    private SegmentNodeBuilder(SegmentNodeBuilder parent, String name) {
        super(parent, name);
        this.base = parent.base.getChildNode(name);
        this.baseRevision = parent.baseRevision;
    }

    @Override
    protected SegmentNodeBuilder createChildBuilder(String name) {
        return new SegmentNodeBuilder(this, name);
    }

    @Override
    public NodeState getBaseState() {
        // TODO: Use the head mechanism in MemoryNodeBuilder instead of
        // overriding base state tracking
        if (baseRevision != ((SegmentNodeBuilder) rootBuilder).baseRevision) {
            base = parent.getBaseState().getChildNode(name);
            baseRevision = ((SegmentNodeBuilder) rootBuilder).baseRevision;
        }
        return base;
    }

}
