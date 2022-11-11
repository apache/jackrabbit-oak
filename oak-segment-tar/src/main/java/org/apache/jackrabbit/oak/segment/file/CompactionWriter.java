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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.SegmentWriterFactory;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.file.CompactedNodeState.PartiallyCompactedNodeState;
import static org.apache.jackrabbit.oak.segment.file.CompactedNodeState.FullyCompactedNodeState;

/**
 * The CompactionWriter delegates compaction calls to the correct {@code SegmentWriter} based on GCGeneration.
 */
public class CompactionWriter {
    private final @NotNull SegmentReader reader;
    private final @Nullable BlobStore blobStore;
    private final @NotNull GCIncrement gcIncrement;
    private final @NotNull SegmentWriter partialWriter;
    private final @NotNull SegmentWriter targetWriter;

    public CompactionWriter(
            @NotNull SegmentReader reader,
            @Nullable BlobStore blobStore,
            @NotNull GCGeneration generation,
            @NotNull SegmentWriter segmentWriter) {
        this.reader = reader;
        this.blobStore = blobStore;
        this.gcIncrement = new GCIncrement(generation, generation, generation);
        this.partialWriter = segmentWriter;
        this.targetWriter = segmentWriter;
    }

    public CompactionWriter(
            @NotNull SegmentReader reader,
            @Nullable BlobStore blobStore,
            @NotNull GCIncrement gcIncrement,
            @NotNull SegmentWriterFactory segmentWriterFactory) {
        this.reader = reader;
        this.blobStore = blobStore;
        this.gcIncrement = gcIncrement;
        this.partialWriter = gcIncrement.createPartialWriter(segmentWriterFactory);
        this.targetWriter = gcIncrement.createTargetWriter(segmentWriterFactory);
    }

    public @NotNull FullyCompactedNodeState writeFullyCompactedNode(
            @NotNull NodeState nodeState,
            @Nullable Buffer stableId
    ) throws IOException {
        RecordId nodeId = targetWriter.writeNode(nodeState, stableId);
        return new FullyCompactedNodeState(reader, targetWriter, blobStore, nodeId);
    }

    public @Nullable PartiallyCompactedNodeState writePartiallyCompactedNode(
            @NotNull NodeState nodeState,
            @Nullable Buffer stableId
    ) throws IOException {
        RecordId nodeId = partialWriter.writeNode(nodeState, stableId);
        return new PartiallyCompactedNodeState(reader, partialWriter, blobStore, nodeId);
    }

    public void flush() throws IOException {
        partialWriter.flush();
        targetWriter.flush();
    }

    public @Nullable FullyCompactedNodeState getPreviouslyCompactedState(NodeState nodeState) {
        if (!(nodeState instanceof SegmentNodeState)) {
            return null;
        }
        SegmentNodeState segmentNodeState = (SegmentNodeState) nodeState;
        if (!gcIncrement.isFullyCompacted(segmentNodeState.getGcGeneration())) {
            return null;
        }
        RecordId nodeId = segmentNodeState.getRecordId();
        return new FullyCompactedNodeState(reader, targetWriter, blobStore, nodeId);
    }
}
