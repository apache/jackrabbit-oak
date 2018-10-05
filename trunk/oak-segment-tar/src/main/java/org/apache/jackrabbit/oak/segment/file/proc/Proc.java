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

package org.apache.jackrabbit.oak.segment.file.proc;

import java.io.InputStream;
import java.util.Optional;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class Proc {

    public interface Backend {

        interface Segment {

            int getGeneration();

            int getFullGeneration();

            boolean isCompacted();

            int getLength();

            int getVersion();

            boolean isDataSegment();

            Optional<String> getInfo();

        }

        interface Commit {

            long getTimestamp();

            String getRevision();

            Optional<NodeState> getRoot();

        }

        interface Record {
            int getNumber();

            String getSegmentId();

            int getOffset();

            int getAddress();

            String getType();

            Optional<NodeState> getRoot();
        }

        boolean tarExists(String name);

        Optional<Long> getTarSize(String name);

        Iterable<String> getTarNames();

        boolean segmentExists(String name, String segmentId);

        Iterable<String> getSegmentIds(String name);

        Optional<Segment> getSegment(String segmentId);

        Optional<InputStream> getSegmentData(String segmentId);

        Optional<Iterable<String>> getSegmentReferences(String segmentId);

        Optional<Iterable<Record>> getSegmentRecords(String segmentId);

        boolean commitExists(String handle);

        Iterable<String> getCommitHandles();

        Optional<Commit> getCommit(String handle);

    }

    public static NodeState of(Backend backend) {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setChildNode("store", new StoreNode(backend));
        builder.setChildNode("journal", new JournalNode(backend));
        return builder.getNodeState();
    }

    private Proc() {
        // Prevent external instantiation.
    }

}
