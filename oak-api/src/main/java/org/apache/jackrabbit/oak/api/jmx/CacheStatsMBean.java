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
package org.apache.jackrabbit.oak.api.jmx;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface CacheStatsMBean {
    String TYPE = "CacheStats";

    String getName();

    /**
     * Returns the number of times {@code com.google.common.cache.Cache} lookup methods have returned either a cached or
     * uncached value. This is defined as {@code getHitCount + getMissCount}.
     */
    long getRequestCount();

    /**
     * Returns the number of times {@code com.google.common.cache.Cache} lookup methods have returned a cached value.
     */
    long getHitCount();

    /**
     * Returns the ratio of cache requests which were hits. This is defined as
     * {@code getHitCount / getRequestCount}, or {@code 1.0} when {@code getRequestCount == 0}.
     * Note that {@code getHitRate + getMissRate =~ 1.0}.
     */
    double getHitRate();
    /**
     * Returns the number of times {@code com.google.common.cache.Cache} lookup methods have returned an uncached (newly
     * loaded) value, or null. Multiple concurrent calls to {@code com.google.common.cache.Cache} lookup methods on an absent
     * value can result in multiple misses, all returning the results of a single cache load
     * operation.
     */
    long getMissCount();

    /**
     * Returns the ratio of cache requests which were misses. This is defined as
     * {@code getMissCount / getRequestCount}, or {@code 0.0} when {@code getRequestCount == 0}.
     * Note that {@code getHitRate + getMissRate =~ 1.0}. Cache misses include all requests which
     * weren't cache hits, including requests which resulted in either successful or failed loading
     * attempts, and requests which waited for other threads to finish loading. It is thus the case
     * that {@code getMissCount &gt;= getLoadSuccessCount + getLoadExceptionCount}. Multiple
     * concurrent misses for the same key will result in a single load operation.
     */
    double getMissRate();

    /**
     * Returns the total number of times that {@code com.google.common.cache.Cache} lookup methods attempted to load new
     * values. This includes both successful load operations, as well as those that threw
     * exceptions. This is defined as {@code getLoadSuccessCount + getLoadExceptionCount}.
     */
    long getLoadCount();

    /**
     * Returns the number of times {@code com.google.common.cache.Cache} lookup methods have successfully loaded a new value.
     * This is always incremented in conjunction with {@link #getMissCount}, though {@code getMissCount}
     * is also incremented when an exception is encountered during cache loading (see
     * {@link #getLoadExceptionCount}). Multiple concurrent misses for the same key will result in a
     * single load operation.
     */
    long getLoadSuccessCount();

    /**
     * Returns the number of times {@code com.google.common.cache.Cache} lookup methods threw an exception while loading a
     * new value. This is always incremented in conjunction with {@code getMissCount}, though
     * {@code getMissCount} is also incremented when cache loading completes successfully (see
     * {@link #getLoadSuccessCount}). Multiple concurrent misses for the same key will result in a
     * single load operation.
     */
    long getLoadExceptionCount();

    /**
     * Returns the ratio of cache loading attempts which threw exceptions. This is defined as
     * {@code getLoadExceptionCount / (getLoadSuccessCount + getLoadExceptionCount)}, or
     * {@code 0.0} when {@code getLoadSuccessCount + getLoadExceptionCount == 0}.
     */
    double getLoadExceptionRate();

    /**
     * Returns the total number of nanoseconds the cache has spent loading new values. This can be
     * used to calculate the miss penalty. This value is increased every time
     * {@code getLoadSuccessCount} or {@code getLoadExceptionCount} is incremented.
     */
    long getTotalLoadTime();

    /**
     * Returns the average time spent loading new values. This is defined as
     * {@code getTotalLoadTime / (getLoadSuccessCount + getLoadExceptionCount)}.
     */
    double getAverageLoadPenalty();

    /**
     * Returns the number of times an entry has been evicted. This count does not include manual
     * {@code com.google.common.cache.Cache#invalidate invalidations}.
     */
    long getEvictionCount();

    /**
     * Get the number of elements/objects in the cache.
     * @return the number of elements
     */
    long getElementCount();

    /**
     * The maximum weight of entries the cache may contain.
     * @return  the maximum total weight of entries the cache may contain
     */
    long getMaxTotalWeight();

    /**
     * Total weight of the complete cache. Depending on implementation it might be the amount
     * of RAM taken by the cache
     * @return to weight of the cache
     */
    //Computing weight is costly hence its an operation
    long estimateCurrentWeight();

    /**
     * Gathers the stats of the cache for logging.
     */
    String cacheInfoAsString();

    /**
     * Reset the cache stats
     */
    void resetStats();
}
