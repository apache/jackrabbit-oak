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

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Simple wrapper class for {@code SegmentNodeState} to keep track of fully and partially compacted nodes.
 */
public abstract class CompactedNodeState extends SegmentNodeState {
    final boolean complete;

    private CompactedNodeState(
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @NotNull RecordId id,
            boolean complete) {
        super(reader, writer, blobStore, id);
        this.complete = complete;
    }

    final public boolean isComplete() {
        return complete;
    }

    final static class FullyCompactedNodeState extends CompactedNodeState {
        FullyCompactedNodeState(
                @NotNull SegmentReader reader,
                @NotNull SegmentWriter writer,
                @Nullable BlobStore blobStore,
                @NotNull RecordId id) {
            super(reader, writer, blobStore, id, true);
        }
    }

    final static class PartiallyCompactedNodeState extends CompactedNodeState {
        PartiallyCompactedNodeState(
                @NotNull SegmentReader reader,
                @NotNull SegmentWriter writer,
                @Nullable BlobStore blobStore,
                @NotNull RecordId id) {
            super(reader, writer, blobStore, id, false);
        }
    }
}
