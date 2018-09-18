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

import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class SegmentNode {

    private SegmentNode() {
        // Prevent instantiation.
    }

    static NodeState newSegmentNode(Backend backend, String segmentId) {
        return backend.getSegment(segmentId)
            .map(segment -> newSegmentNode(backend, segmentId, segment))
            .orElseGet(() -> newMissingSegment(segmentId));
    }

    private static NodeState newSegmentNode(Backend backend, String segmentId, Segment segment) {
        if (segment.isDataSegment()) {
            return new DataSegmentNode(backend, segmentId, segment);
        }
        return new BulkSegmentNode(backend, segmentId, segment);
    }

    private static NodeState newMissingSegment(String segmentId) {
        return new MissingSegmentNode(segmentId);
    }

}
