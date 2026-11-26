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
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import java.nio.ByteBuffer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Synchronous-writer variant of {@link AbstractPersistentDiskCacheV2}. Inside the cache-miss loader, the loaded
 * buffer is written to disk synchronously before the loader returns. If the write succeeds, the file exists
 * before the cache entry is published, so this variant has no pending-writes buffer or orphan-cleanup logic.
 * If the write fails, the caller still receives the segment loaded from the backend.
 * <p>
 * The disk write happens inside {@code cache.get(key, loader)}, so the cache holds the loader lock for the
 * duration of the write. For high-concurrency cold starts, {@link PersistentDiskCacheV2Async} moves that write
 * to a background pool.
 */
public class PersistentDiskCacheV2Sync extends AbstractPersistentDiskCacheV2 {
    public static final String NAME = "Segment Disk Cache (Sync)";

    public PersistentDiskCacheV2Sync(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor) throws IOException {
        super(directory, cacheMaxSizeMB, diskCacheIOMonitor, NAME);
        rehydrateFromDisk();
    }

    @Override
    protected void handleLoadedSegment(UUID segmentId, ByteBuffer buffer) {
        try {
            writeSegmentInternal(segmentId, buffer);
        } catch (IOException e) {
            // The segment was loaded successfully from the backend; only persisting it to the disk cache
            // failed (e.g. the cache volume is full or read-only). Swallow the failure and let the loaded
            // buffer flow back to the caller instead of failing the read. The base loader still
            // records cache membership for this id; since no file was written, the next read of it misses
            // on disk and self-heals via the NoSuchFile -> invalidate -> reload path. This mirrors the
            // asynchronous variant, whose failed background write leaves the same recoverable state.
            logWriteFailure(segmentId, e);
            // A partial write may have advanced the buffer's position
            buffer.rewind();
        }
    }
}
