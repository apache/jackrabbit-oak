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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;

/**
 * This class exposes {@link CounterStats} for allocations and de-allocations
 * of {@link Buffer} instances:
 * <ul>
 *     <li>{@link #DIRECT_BUFFER_COUNT}: number of allocated direct byte
 *          buffers.</li>
 *     <li>{@link #DIRECT_BUFFER_CAPACITY}: total capacity of the allocated
 *          direct byte buffers.</li>
 *     <li>{@link #HEAP_BUFFER_COUNT}: number of allocated heap byte
 *          buffers.</li>
 *     <li>{@link #HEAP_BUFFER_CAPACITY}: total capacity of the allocated
 *          heap byte buffers.</li>
 * </ul>
 * <p>
 * Users of this class call {@link #trackAllocation(Buffer)} to update above statistics.
 */
public class SegmentBufferMonitor {

    /**
     * Number of allocated direct byte buffers
     */
    public static final String DIRECT_BUFFER_COUNT = "oak.segment.direct-buffer-count";

    /**
     * Total capacity of the allocated direct byte buffers.
     */
    public static final String DIRECT_BUFFER_CAPACITY = "oak.segment.direct-buffer-capacity";

    /**
     * Number of allocated heap byte buffers
     */
    public static final String HEAP_BUFFER_COUNT = "oak.segment.heap-buffer-count";

    /**
     * Total capacity of the allocated heap byte buffers.
     */
    public static final String HEAP_BUFFER_CAPACITY = "oak.segment.heap-buffer-capacity";

    @NotNull
    private final Set<BufferReference> buffers = CollectionUtils.newConcurrentHashSet();

    @NotNull
    private final ReferenceQueue<Buffer> referenceQueue = new ReferenceQueue<>();

    @NotNull
    private final CounterStats directBufferCount;

    @NotNull
    private final CounterStats directBufferCapacity;

    @NotNull
    private final CounterStats heapBufferCount;

    @NotNull
    private final CounterStats heapBufferCapacity;

    /**
     * Create a new instance using the passed {@code statisticsProvider} to expose
     * buffer allocations.
     * @param statisticsProvider
     */
    public SegmentBufferMonitor(@NotNull StatisticsProvider statisticsProvider) {
        directBufferCount = statisticsProvider.getCounterStats(DIRECT_BUFFER_COUNT, METRICS_ONLY);
        directBufferCapacity = statisticsProvider.getCounterStats(DIRECT_BUFFER_CAPACITY, METRICS_ONLY);
        heapBufferCount = statisticsProvider.getCounterStats(HEAP_BUFFER_COUNT, METRICS_ONLY);
        heapBufferCapacity = statisticsProvider.getCounterStats(HEAP_BUFFER_CAPACITY, METRICS_ONLY);
    }

    private static class BufferReference extends WeakReference<Buffer> {
        private final int capacity;
        private final boolean isDirect;

        public BufferReference(@NotNull Buffer buffer,
            @NotNull ReferenceQueue<Buffer> queue) {
            super(buffer, queue);
            this.capacity = buffer.capacity();
            this.isDirect = buffer.isDirect();
        }
    }

    /**
     * Track the allocation of a {@code buffer} and update the exposed statistics.
     * @param buffer
     */
    public void trackAllocation(@NotNull Buffer buffer) {
        BufferReference reference = new BufferReference(buffer, referenceQueue);
        buffers.add(reference);
        allocated(reference);
        trackDeallocations();
    }

    private void trackDeallocations() {
        BufferReference reference = (BufferReference) referenceQueue.poll();
        while (reference != null) {
            buffers.remove(reference);
            deallocated(reference);
            reference = (BufferReference) referenceQueue.poll();
        }
    }

    private void allocated(@NotNull BufferReference reference) {
        if (reference.isDirect) {
            directBufferCount.inc();
            directBufferCapacity.inc(reference.capacity);
        } else {
            heapBufferCount.inc();
            heapBufferCapacity.inc(reference.capacity);
        }
    }

    private void deallocated(@NotNull BufferReference reference) {
        if (reference.isDirect) {
            directBufferCount.dec();
            directBufferCapacity.dec(reference.capacity);
        } else {
            heapBufferCount.dec();
            heapBufferCapacity.dec(reference.capacity);
        }
    }

}
