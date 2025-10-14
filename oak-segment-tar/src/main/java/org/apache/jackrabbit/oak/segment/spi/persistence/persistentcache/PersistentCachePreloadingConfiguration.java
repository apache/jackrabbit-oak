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
 * Configuration for a segment preload mechanism that preloads segments into a
 * {@link PersistentCache}. The preload mechanism is triggered whenever a segment
 * in the cache is {@link PersistentCache#readSegment(long, long, Callable)|accessed}.
 * When this happens, all segments referenced by the accessed segment are asynchronously
 * preloaded.
 * <p>
 * Next to the concurrency level, i.e. how many threads are used for preloading, the
 * {@code maxPreloadDepth} (default: {@code 1}, which controls how many recursive levels
 * of referenced segments are preloaded, can be configured.
 * <p>
 * Prefetching is done asynchronously, but it <i>may</i> add some overhead. It is primarily
 * recommended to parallelize slow I/O, e.g. when using a remote persistence.
 * <p>
 * Different scenarios may warrant different preloading strategies. A short-lived
 * process traversing a repository (e.g. copy, offline-compaction) with an initially
 * empty cache may benefit from a more threads and a higher preload-depth, while a
 * long-running process, e.g. a web application, may perform better with fewer threads
 * and a lower preload depth.
 */
public class PersistentCachePreloadingConfiguration {

    private final int concurrency;

    private int maxPreloadDepth;

    private PersistentCachePreloadingConfiguration(int concurrency, int preloadDepth) {
        this.concurrency = concurrency;
        this.maxPreloadDepth = preloadDepth;
    }

    /**
     * Creates a new {@link PersistentCachePreloadingConfiguration} with the given concurrency
     * level and a {@code preloadDepth} of {@code 1}.
     *
     * @param concurrency number of threads to use for preloading
     * @return a new configuration
     */
    public static PersistentCachePreloadingConfiguration withConcurrency(int concurrency) {
        return new PersistentCachePreloadingConfiguration(concurrency, 1);
    }

    /**
     * Set how many recursive levels of referenced segments should be preloaded.
     *
     * @param maxPreloadDepth depth of the preloading, i.e. how many levels of referenced
     *                      segments should be preloaded (default: {@code 1})
     * @return this configuration
     */
    public PersistentCachePreloadingConfiguration withMaxPreloadDepth(int maxPreloadDepth) {
        this.maxPreloadDepth = maxPreloadDepth;
        return this;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getMaxPreloadDepth() {
        return maxPreloadDepth;
    }

    @Override
    public String toString() {
        return "PersistentCachePreloadingConfiguration{" +
                "concurrency=" + concurrency +
                ", maxPreloadDepth=" + maxPreloadDepth +
                '}';
    }
}
