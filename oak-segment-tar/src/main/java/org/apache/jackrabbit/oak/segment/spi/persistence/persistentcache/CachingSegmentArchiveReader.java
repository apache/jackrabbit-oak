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
 *
 */

package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentGraph;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CachingSegmentArchiveReader implements SegmentArchiveReader {

    private static final Logger LOG = LoggerFactory.getLogger(CachingSegmentArchiveReader.class);


    @NotNull
    private final PersistentCache persistentCache;

    @NotNull
    private final SegmentArchiveReader delegate;

    private final ExecutorService prefetchExecutor;
    private final Set<UUID> inFlightPrefetch =
            Collections.newSetFromMap(
                    new ConcurrentHashMap<>());
    private final boolean prefetchEnabled;
    private final int prefetchMaxRefs;



    public CachingSegmentArchiveReader(
            @NotNull PersistentCache persistentCache,
            @NotNull SegmentArchiveReader delegate) {
        this.persistentCache = persistentCache;
        this.delegate = delegate;
        int threads = Integer.getInteger("oak.segment.cache.threads", 10);
        this.prefetchEnabled = Boolean.getBoolean("oak.segment.cache.prefetch.enabled");
        this.prefetchMaxRefs = Integer.getInteger("oak.segment.cache.prefetch.maxRefs", 20);
        this.prefetchExecutor = Executors.newFixedThreadPool(threads);
    }

    @Override
    @Nullable
    public Buffer readSegment(long msb, long lsb) throws IOException {
        Buffer buf = persistentCache.readSegment(msb, lsb, () -> delegate.readSegment(msb, lsb));
        if (buf != null && prefetchEnabled) {
            schedulePrefetch(msb, lsb, buf);
        }
        return buf;
    }

    private List<UUID> extractReferences(Buffer buffer) {
        var data = SegmentData.newSegmentData(buffer);
        int refs = data.getSegmentReferencesCount();
        ArrayList<UUID> out = new ArrayList<>(refs);
        for (int i = 0; i < refs; i++) {
            out.add(new UUID(data.getSegmentReferenceMsb(i), data.getSegmentReferenceLsb(i)));
        }
        return out;
    }

    private void schedulePrefetch(long msb, long lsb, Buffer buffer) {
        try {
            List<UUID> refs = extractReferences(buffer);
            int limit = Math.min(refs.size(), prefetchMaxRefs);
            for (int i = 0; i < limit; i++) {
                final UUID ref = refs.get(i);
                final long rMsb = ref.getMostSignificantBits();
                final long rLsb = ref.getLeastSignificantBits();

                // Skip if already present in cache
                if (persistentCache.containsSegment(rMsb, rLsb)) {
                    continue;
                }

                // Drop prefetch if already in progress for this segment
                boolean registered = inFlightPrefetch.add(ref);
                if (!registered) {
                    continue;
                }

                try {
                    prefetchExecutor.execute(() -> {
                        try {
                            Buffer b = delegate.readSegment(rMsb, rLsb);
                            if (b != null) {
                                // Double-check cache before write to avoid redundant writes
                                if (!persistentCache.containsSegment(rMsb, rLsb)) {
                                    persistentCache.writeSegment(rMsb, rLsb, b);
                                }
                            }
                        } catch (Exception e) {
                            LOG.debug("Prefetch failed for segment {}", new java.util.UUID(rMsb, rLsb), e);
                        } finally {
                            inFlightPrefetch.remove(ref);
                        }
                    });
                } catch (Throwable t) {
                    // If task submission failed (e.g., executor shutting down), undo the registration
                    inFlightPrefetch.remove(ref);
                    LOG.debug("Prefetch submission failed for segment {}", new java.util.UUID(rMsb, rLsb), t);

                }
            }
        } catch (Throwable t) {
            LOG.debug("Prefetch scheduling failed for segment {}", new java.util.UUID(msb, lsb), t);
        }
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        if (persistentCache.containsSegment(msb, lsb)) {
            return true;
        } else {
            return delegate.containsSegment(msb, lsb);
        }
    }

    @Override
    public List<SegmentArchiveEntry> listSegments() {
        return delegate.listSegments();
    }

    @Override
    public @NotNull SegmentGraph getGraph() throws IOException {
        return delegate.getGraph();
    }

    @Override
    @Nullable
    public Buffer getBinaryReferences() throws IOException {
        return delegate.getBinaryReferences();
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    @NotNull
    public String getName() {
        return delegate.getName();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        new ExecutorCloser(prefetchExecutor).close();
    }

    @Override
    public int getEntrySize(int size) {
        return delegate.getEntrySize(size);
    }

    @Override
    public boolean isRemote() {
        return delegate.isRemote();
    }
}