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

import java.io.UnsupportedEncodingException;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.segment.util.SafeEncode;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@code SegmentReader} implementation implements caching for
 * strings and templates. It can also optionally rely on a {@link BlobStore} for resolving blobs.
 */
public class CachingSegmentReader implements SegmentReader {
    public static final int DEFAULT_STRING_CACHE_MB = 256;
    public static final int DEFAULT_TEMPLATE_CACHE_MB = 64;

    private static final Logger LOG = LoggerFactory.getLogger(LoggingHook.class.getName() + ".reader");

    @NotNull
    private final Supplier<SegmentWriter> writer;

    @Nullable
    private final BlobStore blobStore;

    /**
     * Cache for string records
     */
    @NotNull
    private final StringCache stringCache;

    /**
     * Cache for template records
     */
    @NotNull
    private final TemplateCache templateCache;

    private final MeterStats readStats;

    /**
     * Create a new instance based on the supplied arguments.
     * @param writer          A {@code Supplier} for a the {@code SegmentWriter} used by the segment
     *                        builders returned from {@link NodeState#builder()} to write ahead changes.
     *                        {@code writer.get()} must not return {@code null}.
     * @param blobStore       {@code BlobStore} instance of the underlying {@link SegmentStore}, or
     *                        {@code null} if none.
     * @param stringCacheMB   the size of the string cache in MBs or {@code 0} for no cache.
     * @param templateCacheMB the size of the template cache in MBs or {@code 0} for no cache.
     */
    public CachingSegmentReader(
        @NotNull Supplier<SegmentWriter> writer,
        @Nullable BlobStore blobStore,
        long stringCacheMB,
        long templateCacheMB,
        MeterStats readStats
    ) {
        this.writer = checkNotNull(writer);
        this.blobStore = blobStore;
        stringCache = new StringCache(stringCacheMB * 1024 * 1024);
        templateCache = new TemplateCache(templateCacheMB * 1024 * 1024);
        this.readStats = readStats;
    }

    /**
     * Cached reading of a string.
     */
    @NotNull
    @Override
    public String readString(@NotNull RecordId id) {
        final SegmentId segmentId = id.getSegmentId();
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        return stringCache.get(msb, lsb, id.getRecordNumber(), new Function<Integer, String>() {
            @NotNull
            @Override
            public String apply(Integer offset) {
                return segmentId.getSegment().readString(offset);
            }
        });
    }

    @NotNull
    @Override
    public MapRecord readMap(@NotNull RecordId id) {
        return new MapRecord(this, id);
    }

    /**
     * Cached reading of a template.
     */
    @NotNull
    @Override
    public Template readTemplate(@NotNull RecordId id) {
        final SegmentId segmentId = id.getSegmentId();
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        return templateCache.get(msb, lsb, id.getRecordNumber(), new Function<Integer, Template>() {
            @NotNull
            @Override
            public Template apply(Integer offset) {
                return segmentId.getSegment().readTemplate(offset);
            }
        });
    }

    private static String safeEncode(String value) {
        try {
            return SafeEncode.safeEncode(value);
        } catch (UnsupportedEncodingException e) {
            return "ERROR: " + e;
        }
    }

    @NotNull
    @Override
    public SegmentNodeState readNode(@NotNull RecordId id) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} n? {}", Thread.currentThread().getId(), id);
        }
        return new SegmentNodeState(this, writer, blobStore, id, readStats);
    }

    @NotNull
    @Override
    public SegmentNodeState readHeadState(@NotNull Revisions revisions) {
        return readNode(revisions.getHead());
    }

    @NotNull
    @Override
    public SegmentPropertyState readProperty(@NotNull RecordId id, @NotNull PropertyTemplate template) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} p? {}", Thread.currentThread().getId(), id);
        }
        return new SegmentPropertyState(this, id, template);
    }

    @NotNull
    @Override
    public SegmentBlob readBlob(@NotNull RecordId id) {
        return new SegmentBlob(blobStore, id);
    }

    @NotNull
    public CacheStats getStringCacheStats() {
        return stringCache.getStats();
    }

    @NotNull
    public CacheStats getTemplateCacheStats() {
        return templateCache.getStats();
    }
}
