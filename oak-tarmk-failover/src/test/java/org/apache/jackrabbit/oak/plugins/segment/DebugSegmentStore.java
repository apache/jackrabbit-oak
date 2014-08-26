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
package org.apache.jackrabbit.oak.plugins.segment;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import javax.annotation.Nonnull;

public class DebugSegmentStore implements SegmentStore {

    private final SegmentStore target;
    public boolean createReadErrors;

    public DebugSegmentStore(SegmentStore targetStore) {
        this.target = targetStore;
    }

    @Override
    public SegmentTracker getTracker() {
        return this.target.getTracker();
    }

    @Nonnull
    @Override
    public SegmentNodeState getHead() {
        return this.target.getHead();
    }

    @Override
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        return this.target.setHead(base, head);
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        return this.target.containsSegment(id);
    }

    @Override
    public Segment readSegment(SegmentId segmentId) {
        return createReadErrors ? null : this.target.readSegment(segmentId);
    }

    @Override
    public void writeSegment(SegmentId id, byte[] bytes, int offset, int length) {
        this.target.writeSegment(id, bytes, offset, length);
    }

    @Override
    public void close() {
        this.target.close();
    }

    @Override
    public Blob readBlob(String reference) {
        return this.target.readBlob(reference);
    }

    @Override
    public BlobStore getBlobStore() {
        return this.target.getBlobStore();
    }

    @Override
    public void gc() {
        this.target.gc();
    }
}
