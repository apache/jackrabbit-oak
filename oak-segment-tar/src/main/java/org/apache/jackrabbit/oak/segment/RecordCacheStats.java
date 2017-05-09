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

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;

/**
 * Statistics for {@link RecordCache}.
 */
public class RecordCacheStats extends AbstractCacheStats {

    @Nonnull
    private final Supplier<CacheStats> stats;

    @Nonnull
    private final Supplier<Long> elementCount;

    @Nonnull
    private final Supplier<Long> weight;

    public RecordCacheStats(
            @Nonnull String name,
            @Nonnull Supplier<CacheStats> stats,
            @Nonnull Supplier<Long> elementCount,
            @Nonnull Supplier<Long> weight) {
        super(name);
        this.stats = checkNotNull(stats);
        this.elementCount = checkNotNull(elementCount);
        this.weight = checkNotNull(weight);
    }

    @Override
    protected CacheStats getCurrentStats() {
        return stats.get();
    }

    @Override
    public long getElementCount() {
        return elementCount.get();
    }

    @Override
    public long getMaxTotalWeight() {
        return -1;
    }

    @Override
    public long estimateCurrentWeight() {
        return weight.get();
    }
}
