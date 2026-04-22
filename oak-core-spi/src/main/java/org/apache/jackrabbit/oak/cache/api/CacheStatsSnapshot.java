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
package org.apache.jackrabbit.oak.cache.api;

import org.jetbrains.annotations.NotNull;

/**
 * An immutable snapshot of cache statistics at a point in time.
 *
 * <p>Returned by {@link Cache#stats()}. All counters are cumulative since
 * the cache was created. Use {@link #minus(CacheStatsSnapshot)} to compute a delta
 * between two snapshots.</p>
 *
 * @param hitCount         number of times a requested key was found in the cache
 * @param missCount        number of times a requested key was not found in the cache
 * @param loadSuccessCount number of times a new value was successfully loaded
 * @param loadFailureCount number of times a value load attempt threw an exception
 * @param totalLoadTime    total time spent loading new values, in nanoseconds
 * @param evictionCount    number of entries evicted from the cache
 */
public record CacheStatsSnapshot(
        long hitCount,
        long missCount,
        long loadSuccessCount,
        long loadFailureCount,
        long totalLoadTime,
        long evictionCount) {

    /**
     * Returns the total number of requests (hits + misses).
     *
     * @return request count
     */
    public long requestCount() {
        return hitCount + missCount;
    }

    /**
     * Returns the ratio of cache requests that were hits, or {@code 1.0} if no
     * requests have been made.
     *
     * @return hit rate between 0.0 and 1.0
     */
    public double hitRate() {
        long requests = requestCount();
        return requests == 0 ? 1.0 : (double) hitCount / requests;
    }

    /**
     * Returns the ratio of cache requests that were misses, or {@code 0.0} if
     * no requests have been made.
     *
     * @return miss rate between 0.0 and 1.0
     */
    public double missRate() {
        long requests = requestCount();
        return requests == 0 ? 0.0 : (double) missCount / requests;
    }

    /**
     * Returns the difference between this snapshot and an earlier {@code other}
     * snapshot, useful for computing per-interval deltas.
     *
     * @param other the earlier snapshot to subtract (must not be null)
     * @return a new snapshot representing the delta
     */
    @NotNull
    public CacheStatsSnapshot minus(@NotNull CacheStatsSnapshot other) {
        return new CacheStatsSnapshot(
                Math.max(0, hitCount - other.hitCount),
                Math.max(0, missCount - other.missCount),
                Math.max(0, loadSuccessCount - other.loadSuccessCount),
                Math.max(0, loadFailureCount - other.loadFailureCount),
                Math.max(0, totalLoadTime - other.totalLoadTime),
                Math.max(0, evictionCount - other.evictionCount)
        );
    }
}
