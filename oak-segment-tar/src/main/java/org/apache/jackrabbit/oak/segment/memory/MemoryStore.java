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

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * A store used for in-memory operations.
 */
public class MemoryStore implements SegmentStore {

    @Nonnull
    private final SegmentTracker tracker;

    @Nonnull
    private final MemoryStoreRevisions revisions;

    @Nonnull
    private final SegmentReader segmentReader;

    @Nonnull
    private final SegmentWriter segmentWriter;

    private final ConcurrentMap<SegmentId, Segment> segments =
            Maps.newConcurrentMap();

    public MemoryStore() throws IOException {
        this.tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override @Nonnull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(MemoryStore.this, msb, lsb);
            }
        });
        this.revisions = new MemoryStoreRevisions();
        this.segmentReader = new CachingSegmentReader(this::getWriter, null, 16, 2);
        this.segmentWriter = defaultSegmentWriterBuilder("sys").withWriterPool().build(this);
        revisions.bind(this);
        segmentWriter.flush();
    }

    @Nonnull
    public SegmentWriter getWriter() {
        return segmentWriter;
    }

    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    @Nonnull
    public SegmentIdProvider getSegmentIdProvider() {
        return tracker;
    }

    @Nonnull
    public Revisions getRevisions() {
        return revisions;
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
        Segment segment = new Segment(tracker, segmentReader, id, buffer);
        if (segments.putIfAbsent(id, segment) != null) {
            throw new IOException("Segment override: " + id);
        }
    }

    /**
     * @return  {@code null}
     */
    @CheckForNull
    public BlobStore getBlobStore() {
        return null;
    }

    public void gc() {
        System.gc();
        segments.keySet().retainAll(tracker.getReferencedSegmentIds());
    }

    public Set<SegmentId> getReferencedSegmentIds() {
        return tracker.getReferencedSegmentIds();
    }
}
