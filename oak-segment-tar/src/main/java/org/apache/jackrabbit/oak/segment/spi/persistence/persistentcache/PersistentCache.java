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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Callable;

/**
 * This interface represents a cache which survives segment store restarts.
 * The cache is agnostic to any archive structure. Segments are only
 * identified by their UUIDs, specified as msb and lsb parts of the segment id.
 */
public interface PersistentCache {

    /**
     * Reads the segment from cache.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @param loader in case of cache miss, with {@code loader.call()} missing element will be retrieved
     * @return byte buffer containing the segment data or null if the segment doesn't exist
     */
    @Nullable
    Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader);

    /**
     * Check if the segment exists in the cache.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @return {@code true} if the segment exists
     */
    boolean containsSegment(long msb, long lsb);

    /**
     * Writes the segment to the cache.
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @param buffer the byte buffer containing the segment data
     */
    void writeSegment(long msb, long lsb, Buffer buffer);

    /**
     * Purges the cache entries according to the implementation policy (e.g. maximum
     * cache size, maximum number of entries, etc.)
     */
    void cleanUp();
}
