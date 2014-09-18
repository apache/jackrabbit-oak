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
package org.apache.jackrabbit.oak.plugins.segment.failover.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverStore implements SegmentStore {
    private static final Logger log = LoggerFactory.getLogger(FailoverStore.class);

    private final SegmentTracker tracker = new SegmentTracker(this);

    private final SegmentStore delegate;

    private RemoteSegmentLoader loader;

    public FailoverStore(SegmentStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Override
    public SegmentNodeState getHead() {
        return delegate.getHead();
    }

    @Override
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        return delegate.setHead(base, head);
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        return delegate.containsSegment(id);
    }

    @Override
    public Segment readSegment(SegmentId sid) {
        log.info("shall read segment " + sid);
        Deque<SegmentId> ids = new ArrayDeque<SegmentId>();
        ids.offer(sid);
        int err = 0;
        Set<SegmentId> seen = new HashSet<SegmentId>();

        while (!ids.isEmpty()) {
            SegmentId id = ids.remove();
            if (!seen.contains(id) && !delegate.containsSegment(id)) {
                log.debug("trying to read segment " + id);
                Segment s = loader.readSegment(id.toString());
                if (s != null) {
                    log.info("got segment " + id + " with size " + s.size());
                    ByteArrayOutputStream bout = new ByteArrayOutputStream(
                            s.size());
                    if (id.isDataSegmentId()) {
                        ids.addAll(s.getReferencedIds());
                    }
                    try {
                        s.writeTo(bout);
                        writeSegment(id, bout.toByteArray(), 0, s.size());
                    } catch (IOException e) {
                        throw new IllegalStateException(
                                "Unable to write remote segment " + id, e);
                    }
                    seen.add(id);
                    ids.removeAll(seen);
                    err = 0;
                } else {
                    log.error("could NOT read segment " + id);
                    if (loader.isClosed() || err == 4) {
                        loader.close();
                        throw new IllegalStateException(
                                "Unable to load remote segment " + id);
                    }
                    err++;
                    ids.addFirst(id);
                }
            } else {
                seen.add(id);
            }
        }

        log.info("calling delegate to return segment " + sid);
        return delegate.readSegment(sid);
    }

    @Override
    public void writeSegment(SegmentId id, byte[] bytes, int offset, int length) {
        delegate.writeSegment(id, bytes, offset, length);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Blob readBlob(String reference) {
        return delegate.readBlob(reference);
    }

    @Override
    public BlobStore getBlobStore() {
        return delegate.getBlobStore();
    }

    @Override
    public void gc() {
        delegate.gc();
    }

    public void setLoader(RemoteSegmentLoader loader) {
        this.loader = loader;
    }

}
