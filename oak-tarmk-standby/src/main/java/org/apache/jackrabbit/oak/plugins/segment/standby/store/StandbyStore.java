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
package org.apache.jackrabbit.oak.plugins.segment.standby.store;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class StandbyStore implements SegmentStore {

    private static final Logger log = LoggerFactory.getLogger(StandbyStore.class);

    private final SegmentTracker tracker = new SegmentTracker(this);

    private final SegmentStore delegate;

    private RemoteSegmentLoader loader;

    @Deprecated
    public StandbyStore(SegmentStore delegate) {
        this.delegate = delegate;
    }

    @Override
    @Deprecated
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Override
    @Deprecated
    public SegmentNodeState getHead() {
        return delegate.getHead();
    }

    @Override
    @Deprecated
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        return delegate.setHead(base, head);
    }

    @Override
    @Deprecated
    public boolean containsSegment(SegmentId id) {
        return delegate.containsSegment(id);
    }

    @Override
    @Deprecated
    public Segment readSegment(SegmentId sid) {
        callId++;
        Deque<SegmentId> ids = new ArrayDeque<SegmentId>();
        ids.offer(sid);
        int err = 0;
        Set<SegmentId> persisted = new HashSet<SegmentId>();

        Map<SegmentId, Segment> cache = new HashMap<SegmentId, Segment>();
        long cacheOps = 0;

        long cacheWeight = 0;
        long maxWeight = 0;
        long maxKeys = 0;

        Set<SegmentId> visited = newHashSet();

        while (!ids.isEmpty()) {
            SegmentId id = ids.remove();

            visited.add(id);

            if (!persisted.contains(id) && !delegate.containsSegment(id)) {
                Segment s;
                boolean logRefs = true;
                if (cache.containsKey(id)) {
                    s = cache.remove(id);
                    cacheWeight -= s.size();
                    cacheOps++;
                    logRefs = false;
                } else {
                    log.debug("transferring segment {}", id);
                    s = loader.readSegment(id.toString());
                }

                if (s != null) {
                    log.debug("processing segment {} with size {}", id,
                            s.size());
                    if (id.isDataSegmentId()) {
                        boolean hasPendingRefs = false;
                        List<SegmentId> refs = s.getReferencedIds();
                        if (logRefs) {
                            log.debug("{} -> {}", id, refs);
                        }
                        for (SegmentId nr : refs) {
                            // skip already persisted or self-ref
                            if (persisted.contains(nr) || id.equals(nr) || visited.contains(nr)) {
                                continue;
                            }
                            hasPendingRefs = true;
                            if (!ids.contains(nr)) {
                                if (nr.isBulkSegmentId()) {
                                    // binaries first
                                    ids.addFirst(nr);
                                } else {
                                    // data segments last
                                    ids.add(nr);
                                }
                            }
                        }

                        if (!hasPendingRefs) {
                            persisted.add(id);
                            persist(id, s);
                        } else {
                            // persist it later, after the refs are in place
                            ids.add(id);

                            // TODO there is a chance this might introduce
                            // a OOME because of the position of the current
                            // segment in the processing queue. putting it at
                            // the end of the current queue means it will stay
                            // in the cache until the pending queue of the
                            // segment's references is processed.
                            cache.put(id, s);
                            cacheWeight += s.size();
                            cacheOps++;

                            maxWeight = Math.max(maxWeight, cacheWeight);
                            maxKeys = Math.max(maxKeys, cache.size());
                        }
                    } else {
                        persisted.add(id);
                        persist(id, s);
                    }
                    ids.removeAll(persisted);
                    err = 0;
                } else {
                    log.error("could NOT read segment {}", id);
                    if (loader.isClosed() || err == 4) {
                        loader.close();
                        throw new IllegalStateException(
                                "Unable to load remote segment " + id);
                    }
                    err++;
                    ids.addFirst(id);
                }
            } else {
                persisted.add(id);
            }
        }
        cacheStats.put(callId, "W: " + humanReadableByteCount(maxWeight)
                + ", Keys: " + maxKeys + ", Ops: " + cacheOps);
        return delegate.readSegment(sid);
    }

    @Deprecated
    public void persist(SegmentId in, Segment s) {
        SegmentId id = delegate.getTracker().getSegmentId(
                in.getMostSignificantBits(), in.getLeastSignificantBits());
        log.debug("persisting segment {} with size {}", id, s.size());
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream(s.size());
            s.writeTo(bout);
            writeSegment(id, bout.toByteArray(), 0, s.size());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write remote segment "
                    + id, e);
        }
    }

    private long callId = 0;
    private Map<Long, String> cacheStats;

    @Deprecated
    public void preSync(RemoteSegmentLoader loader) {
        this.loader = loader;
        this.cacheStats = new HashMap<Long, String>();
    }

    @Deprecated
    public void postSync() {
        loader = null;
        if (log.isDebugEnabled() && !cacheStats.isEmpty()) {
            log.debug("sync cache stats {}", cacheStats);
        }
        cacheStats = null;
    }

    @Override
    @Deprecated
    public void writeSegment(SegmentId id, byte[] bytes, int offset, int length) throws IOException {
        delegate.writeSegment(id, bytes, offset, length);
    }

    @Override
    @Deprecated
    public void close() {
        delegate.close();
    }

    @Override
    @Deprecated
    public Blob readBlob(String reference) {
        return delegate.readBlob(reference);
    }

    @Override
    @Deprecated
    public BlobStore getBlobStore() {
        return delegate.getBlobStore();
    }

    @Override
    @Deprecated
    public void gc() {
        delegate.gc();
    }

    @Deprecated
    public long size() {
        if (delegate instanceof FileStore) {
            return ((FileStore) delegate).size();
        }
        return -1;
    }

    @Deprecated
    public void cleanup() {
        if (delegate instanceof FileStore) {
            try {
                delegate.getTracker().getWriter().dropCache();
                ((FileStore) delegate).flush(true);
            } catch (IOException e) {
                log.error("Error running cleanup", e);
            }
        } else {
            log.warn("Delegate is not a FileStore, ignoring cleanup call");
        }
    }
}
