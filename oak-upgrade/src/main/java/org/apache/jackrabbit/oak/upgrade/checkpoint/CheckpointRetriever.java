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
package org.apache.jackrabbit.oak.upgrade.checkpoint;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.document.DocumentCheckpointRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.segment.CheckpointAccessor;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.SegmentTarFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public final class CheckpointRetriever {

    public static class Checkpoint implements Comparable<Checkpoint> {

        private final String name;

        private final long created;

        private final long expiryTime;

        public Checkpoint(String name, long created, long expiryTime) {
            this.name = name;
            this.created = created;
            this.expiryTime = expiryTime;
        }

        public static Checkpoint createFromSegmentNode(String name, NodeState node) {
            return new Checkpoint(name, node.getLong("created"), node.getLong("timestamp"));
        }

        public String getName() {
            return name;
        }

        public long getExpiryTime() {
            return expiryTime;
        }

        @Override
        public int compareTo(Checkpoint o) {
            return Long.compare(this.created, o.created);
        }
    }

    private CheckpointRetriever() {
    }

    public static List<Checkpoint> getCheckpoints(NodeStore nodeStore) {
        List<Checkpoint> result;
        if (nodeStore instanceof SegmentNodeStore) {
            result = getCheckpoints(CheckpointAccessor.getCheckpointsRoot((SegmentNodeStore) nodeStore));
        } else if (nodeStore instanceof org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore) {
            result = getCheckpoints(org.apache.jackrabbit.oak.plugins.segment.CheckpointAccessor.getCheckpointsRoot((org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore) nodeStore));
        } else if (nodeStore instanceof DocumentNodeStore) {
            result = DocumentCheckpointRetriever.getCheckpoints((DocumentNodeStore) nodeStore);
        } else if (nodeStore instanceof SegmentTarFactory.NodeStoreWithFileStore) {
            result = getCheckpoints(CheckpointAccessor.getCheckpointsRoot(((SegmentTarFactory.NodeStoreWithFileStore) nodeStore).getNodeStore()));
        } else {
            return null;
        }
        Collections.sort(result);
        return result;
    }

    private static List<Checkpoint> getCheckpoints(NodeState checkpointRoot) {
        return Lists.newArrayList(Iterables.transform(checkpointRoot.getChildNodeEntries(), new Function<ChildNodeEntry, Checkpoint>() {
            @Nullable
            @Override
            public Checkpoint apply(@Nullable ChildNodeEntry input) {
                return Checkpoint.createFromSegmentNode(input.getName(), input.getNodeState());
            }
        }));
    }
}