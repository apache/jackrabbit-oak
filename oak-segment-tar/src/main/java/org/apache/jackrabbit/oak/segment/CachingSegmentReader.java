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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Long.getLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

// FIXME OAK-4451: Implement a proper template cache
// - move the template cache into this class, implement monitoring, management, logging, tests
public class CachingSegmentReader implements SegmentReader {
    public static final int DEFAULT_STRING_CACHE_MB = 256;

    public static final String STRING_CACHE_MB = "oak.segment.stringCacheMB";

    @Nonnull
    private final Supplier<SegmentWriter> writer;

    @Nonnull
    private final Revisions revisions;

    @CheckForNull
    private final BlobStore blobStore;

    /**
     * Cache for string records
     */
    @Nonnull
    private final StringCache stringCache;

    public CachingSegmentReader(
            @Nonnull Supplier<SegmentWriter> writer,
            @Nonnull Revisions revisions,
            @Nullable BlobStore blobStore,
            long stringCacheMB) {
        this.writer = checkNotNull(writer);
        this.revisions = checkNotNull(revisions);
        this.blobStore = blobStore;
        stringCache = new StringCache(getLong(STRING_CACHE_MB, stringCacheMB) * 1024 * 1024);
    }

    @Nonnull
    @Override
    public String readString(@Nonnull RecordId id) {
        final SegmentId segmentId = id.getSegmentId();
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        return stringCache.getString(msb, lsb, id.getOffset(), new Function<Integer, String>() {
            @Nullable
            @Override
            public String apply(Integer offset) {
                return segmentId.getSegment().readString(offset);
            }
        });
    }

    @Nonnull
    @Override
    public MapRecord readMap(@Nonnull RecordId id) {
        return new MapRecord(this, id);
    }

    @Nonnull
    @Override
    public Template readTemplate(@Nonnull RecordId id) {
        int offset = id.getOffset();
        if (id.getSegment().templates == null) {
            return id.getSegment().readTemplate(offset);
        }
        Template template = id.getSegment().templates.get(offset);
        if (template == null) {
            template = id.getSegment().readTemplate(offset);
            id.getSegment().templates.putIfAbsent(offset, template); // only keep the first copy
        }
        return template;
    }

    @Nonnull
    @Override
    public SegmentNodeState readNode(@Nonnull RecordId id) {
        return new SegmentNodeState(this, writer, id);
    }

    @Nonnull
    @Override
    public SegmentNodeState readHeadState() {
        return readNode(revisions.getHead());
    }

    @Nonnull
    @Override
    public SegmentPropertyState readProperty(
            @Nonnull RecordId id, @Nonnull PropertyTemplate template) {
        return new SegmentPropertyState(this, id, template);
    }

    @Nonnull
    @Override
    public SegmentBlob readBlob(@Nonnull RecordId id) {
        return new SegmentBlob(blobStore, id);
    }

    @Nonnull
    public CacheStats getStringCacheStats() {
        return stringCache.getStats();
    }
}
