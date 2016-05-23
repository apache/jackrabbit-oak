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
package org.apache.jackrabbit.oak.segment.memory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriterPool;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentReaderImpl;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A store used for in-memory operations.
 */
public class MemoryStore implements SegmentStore {

    @Nonnull
    private final SegmentTracker tracker = new SegmentTracker(this);

    @Nonnull
    private final SegmentWriter segmentWriter = new SegmentWriter(this,
            new SegmentBufferWriterPool(this, LATEST_VERSION, "sys"));

    @Nonnull
    private final SegmentReader segmentReader = new SegmentReaderImpl(this, 16);

    private SegmentNodeState head;

    private final ConcurrentMap<SegmentId, Segment> segments =
            Maps.newConcurrentMap();

    public MemoryStore(NodeState root) throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("root", root);

        this.head = segmentWriter.writeNode(builder.getNodeState());
        segmentWriter.flush();
    }

    public MemoryStore() throws IOException {
        this(EMPTY_NODE);
    }

    @Override
    @Nonnull
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Override
    @Nonnull
    public SegmentWriter getWriter() {
        return segmentWriter;
    }

    @Override
    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    @Override
    public synchronized SegmentNodeState getHead() {
        return head;
    }

    @Override
    public synchronized boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        if (this.head.getRecordId().equals(base.getRecordId())) {
            this.head = head;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        return id.sameStore(this) || segments.containsKey(id);
    }

    @Override @Nonnull
    public Segment readSegment(SegmentId id) {
        Segment segment = segments.get(id);
        if (segment != null) {
            return segment;
        }
        throw new SegmentNotFoundException(id);
    }

    @Override
    public void writeSegment(
            SegmentId id, byte[] data, int offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(data, offset, length);
        buffer.rewind();
        Segment segment = new Segment(this, id, buffer);
        if (segments.putIfAbsent(id, segment) != null) {
            throw new IOException("Segment override: " + id);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Blob readBlob(String reference) {
        return null;
    }

    @Override
    public BlobStore getBlobStore() {
        return null;
    }

    @Override
    public void gc() {
        System.gc();
        segments.keySet().retainAll(tracker.getReferencedSegmentIds());
    }

}
