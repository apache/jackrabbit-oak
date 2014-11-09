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

public class SegmentNodeBuilder extends MemoryNodeBuilder {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the underlying segments.
     */
    private static final int UPDATE_LIMIT =
            Integer.getInteger("update.limit", 10000);

    private final SegmentWriter writer;

    private long updateCount = 0;

    SegmentNodeBuilder(SegmentNodeState base) {
        this(base, base.getTracker().getWriter());
    }

    SegmentNodeBuilder(SegmentNodeState base, SegmentWriter writer) {
        super(base);
        this.writer = writer;
    }

    //-------------------------------------------------< MemoryNodeBuilder >--

    @Override
    protected void updated() {
        updateCount++;
        if (updateCount > UPDATE_LIMIT) {
            getNodeState();
        }
    }

    //-------------------------------------------------------< NodeBuilder >--

    @Override
    public SegmentNodeState getBaseState() {
        // guaranteed to be a SegmentNodeState
        return (SegmentNodeState) super.getBaseState();
    }

    @Override
    public SegmentNodeState getNodeState() {
        NodeState state = super.getNodeState();
        SegmentNodeState sstate = writer.writeNode(state);
        if (state != sstate) {
            set(sstate);
            updateCount = 0;
        }
        return sstate;
    }

}
