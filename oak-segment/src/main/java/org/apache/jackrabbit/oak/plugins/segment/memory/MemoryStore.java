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
package org.apache.jackrabbit.oak.plugins.segment.memory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentVersion;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Maps;

/**
 * A store used for in-memory operations.
 */
@Deprecated
public class MemoryStore implements SegmentStore {

    private final SegmentTracker tracker = new SegmentTracker(this, 16, SegmentVersion.V_11);

    private SegmentNodeState head;

    private final ConcurrentMap<SegmentId, Segment> segments =
            Maps.newConcurrentMap();

    @Deprecated
    public MemoryStore(NodeState root) throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("root", root);

        SegmentWriter writer = tracker.getWriter();
        this.head = writer.writeNode(builder.getNodeState());
        writer.flush();
    }

    @Deprecated
    public MemoryStore() throws IOException {
        this(EMPTY_NODE);
    }

    @Override
    @Deprecated
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Override
    @Deprecated
    public synchronized SegmentNodeState getHead() {
        return head;
    }

    @Override
    @Deprecated
    public synchronized boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        if (this.head.getRecordId().equals(base.getRecordId())) {
            this.head = head;
            return true;
        } else {
            return false;
        }
    }

    @Override
    @Deprecated
    public boolean containsSegment(SegmentId id) {
        return id.getTracker() == tracker || segments.containsKey(id);
    }

    @Override @Nonnull
    @Deprecated
    public Segment readSegment(SegmentId id) {
        Segment segment = segments.get(id);
        if (segment != null) {
            return segment;
        }
        throw new SegmentNotFoundException(id);
    }

    @Override
    @Deprecated
    public void writeSegment(
            SegmentId id, byte[] data, int offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(data, offset, length);
        buffer.rewind();
        Segment segment = new Segment(tracker, id, buffer);
        if (segments.putIfAbsent(id, segment) != null) {
            throw new IOException("Segment override: " + id);
        }
    }

    @Override
    @Deprecated
    public void close() {
    }

    @Override
    @Deprecated
    public Blob readBlob(String reference) {
        return null;
    }

    @Override
    @Deprecated
    public BlobStore getBlobStore() {
        return null;
    }

    @Override
    @Deprecated
    public void gc() {
        System.gc();
        segments.keySet().retainAll(tracker.getReferencedSegmentIds());
    }

}
