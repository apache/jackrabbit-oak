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

import com.google.common.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

public class SegmentCacheStats extends AbstractCacheStats {
    private final @NotNull Supplier<Long> maximumWeight;

    @NotNull
    private final Supplier<Long> elementCount;

    @NotNull
    final Supplier<Long> currentWeight;

    @NotNull
    final AtomicLong loadSuccessCount = new AtomicLong();

    @NotNull
    final AtomicInteger loadExceptionCount = new AtomicInteger();

    @NotNull
    final AtomicLong loadTime = new AtomicLong();

    @NotNull
    final Supplier<Long> evictionCount;

    @NotNull
    final AtomicLong hitCount = new AtomicLong();

    @NotNull
    final AtomicLong missCount = new AtomicLong();

    public SegmentCacheStats(@NotNull String name,
                             @NotNull Supplier<Long> maximumWeight,
                             @NotNull Supplier<Long> elementCount,
                             @NotNull Supplier<Long> currentWeight,
                             @NotNull Supplier<Long> evictionCount) {
        super(name);
        this.maximumWeight = maximumWeight;
        this.elementCount = checkNotNull(elementCount);
        this.currentWeight = checkNotNull(currentWeight);
        this.evictionCount = evictionCount;
    }

    @Override
    protected CacheStats getCurrentStats() {
        return new CacheStats(
                hitCount.get(),
                missCount.get(),
                loadSuccessCount.get(),
                loadExceptionCount.get(),
                loadTime.get(),
                evictionCount.get()
        );
    }

    @Override
    public long getElementCount() {
        return elementCount.get();
    }

    @Override
    public long getMaxTotalWeight() {
        return maximumWeight.get();
    }

    @Override
    public long estimateCurrentWeight() {
        return currentWeight.get();
    }
}

