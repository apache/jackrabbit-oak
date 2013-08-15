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

import org.apache.jackrabbit.oak.spi.state.NodeState;

class SegmentRootBuilder extends SegmentNodeBuilder {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the private branch.
     */
    private static final int UPDATE_LIMIT =
            Integer.getInteger("update.limit", 1000);

    private final SegmentWriter writer;

    private long updateCount = 0;

    SegmentRootBuilder(SegmentNodeState base, SegmentWriter writer) {
        super(base);
        this.writer = writer;
    }

    @Override
    protected void updated() {
        updateCount++;
        if (updateCount > UPDATE_LIMIT) {
            getNodeState(); // flush changes
            updateCount = 0;
        }
    }

    // TODO: Allow flushing of also non-root builders
    @Override
    public SegmentNodeState getNodeState() {
        SegmentNodeState state = writer.writeNode(super.getNodeState());
        writer.flush();
        super.reset(state);
        return state;
    }

    @Override
    public void reset(NodeState newBase) {
        base = newBase;
        baseRevision++;
        super.reset(newBase);
    }

}
