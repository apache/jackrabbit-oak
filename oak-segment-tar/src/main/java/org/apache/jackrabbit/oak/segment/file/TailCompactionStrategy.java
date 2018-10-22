/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType.TAIL;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class TailCompactionStrategy extends AbstractCompactionStrategy {

    @Override
    GCType getCompactionType() {
        return TAIL;
    }

    @Override
    GCGeneration nextGeneration(GCGeneration current) {
        return current.nextTail();
    }

    @Override
    public CompactionResult compact(Context context) {
        NodeState base = getBase(context);

        if (base == null) {
            context.getGCListener().info("no base state available, tail compaction is not applicable");
            return CompactionResult.notApplicable(context.getGCCount());
        }

        return compact(context, base);
    }

    private static NodeState getBase(Context context) {
        RecordId id = getLastCompactedRootId(context);

        if (RecordId.NULL.equals(id)) {
            return null;
        }

        // Nodes are read lazily. In order to force a read operation for the requested
        // node, the property count is computed. Computing the property count requires
        // access to the node template, whose ID is stored in the content of the node.
        // Accessing the content of the node forces a read operation for the segment
        // containing the node. If the following code completes without throwing a
        // SNFE, we can be sure that *at least* the root node can be accessed. This
        // doesn't say anything about the health of the full closure of the head
        // state.

        try {
            NodeState node = getLastCompactedRootNode(context);
            node.getPropertyCount();
            return node;
        } catch (SegmentNotFoundException e) {
            context.getGCListener().error("base state " + id + " is not accessible", e);
            return null;
        }
    }

    private static String getLastCompactedRoot(Context context) {
        return context.getGCJournal().read().getRoot();
    }

    private static RecordId getLastCompactedRootId(Context context) {
        return RecordId.fromString(context.getSegmentTracker(), getLastCompactedRoot(context));
    }

    private static NodeState getLastCompactedRootNode(Context context) {
        return context.getSegmentReader().readNode(getLastCompactedRootId(context));
    }

}
