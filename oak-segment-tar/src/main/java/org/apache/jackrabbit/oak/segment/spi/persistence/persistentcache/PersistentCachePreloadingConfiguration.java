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
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import java.util.concurrent.Callable;

/**
 * Configuration for a segment prefetch mechanism that preloads segments into a
 * {@link PersistentCache}. The prefetch mechanism is triggered whenever a segment
 * in the cache is {@link PersistentCache#readSegment(long, long, Callable)|accessed}.
 * When this happens, all segments referenced by the accessed segment are asynchronously
 * prefetched.
 * <p>
 * Next to the concurrency level, i.e. how many threads are used for prefetching, the
 * {@code prefetchDepth} (default: {@code 1}, which controls how many recursive levels
 * of referenced segments are prefetched, can be configured.
 * <p>
 * Prefetching is done asynchronously, but it <i>may</i> add some overhead. It is primarily
 * recommended to parallelize slow I/O, e.g. when using a remote persistence.
 * <p>
 * Different scenarios may warrant different prefetching strategies. A short-lived
 * process traversing a repository (e.g. copy, offline-compaction) with an initially
 * empty cache may benefit from a more threads and a higher prefetch-depth, while a
 * long-running process, e.g. a web application, may perform better with fewer threads
 * and a lower prefetch depth.
 */
public class PersistentCachePreloadingConfiguration {

    private final int concurrency;

    private int prefetchDepth;

    private PersistentCachePreloadingConfiguration(int concurrency, int prefetchDepth) {
        this.concurrency = concurrency;
        this.prefetchDepth = prefetchDepth;
    }

    /**
     * Creates a new {@link PersistentCachePreloadingConfiguration} with the given concurrency
     * level and a {@code prefetchDepth} of {@code 1}.
     *
     * @param concurrency number of threads to use for prefetching
     * @return a new configuration
     */
    public static PersistentCachePreloadingConfiguration withConcurrency(int concurrency) {
        return new PersistentCachePreloadingConfiguration(concurrency, 1);
    }

    /**
     * Set how many recursive levels of referenced segments should be prefetched.
     *
     * @param prefetchDepth depth of the prefetching, i.e. how many levels of referenced
     *                      segments should be prefetched (default: {@code 1})
     * @return this configuration
     */
    public PersistentCachePreloadingConfiguration withPrefetchDepth(int prefetchDepth) {
        this.prefetchDepth = prefetchDepth;
        return this;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getPrefetchDepth() {
        return prefetchDepth;
    }

    @Override
    public String toString() {
        return "PersistentCachePreloadingConfiguration{" +
                "concurrency=" + concurrency +
                ", prefetchDepth=" + prefetchDepth +
                '}';
    }
}
