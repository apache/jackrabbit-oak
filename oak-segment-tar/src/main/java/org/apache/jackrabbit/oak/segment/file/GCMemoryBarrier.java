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
package org.apache.jackrabbit.oak.segment.file;

import static java.lang.management.ManagementFactory.getMemoryMXBean;
import static java.lang.management.ManagementFactory.getMemoryPoolMXBeans;
import static java.lang.management.MemoryType.HEAP;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;

/**
 * Responsible for raising the low memory flag whenever the available memory
 * falls under a specified threshold. Uses {@link MemoryPoolMXBean} to register
 * for memory related notifications.
 */
public class GCMemoryBarrier implements Closeable {

    // TODO possibly add a min value to the percentage, ie. skip gc if available
    // heap drops under 2GB

    @Nonnull
    private final AtomicBoolean sufficientMemory;

    @Nonnull
    private final GCListener gcListener;

    @Nonnull
    private final SegmentGCOptions gcOptions;

    private final long gcCount;

    private final NotificationEmitter emitter;
    private final MemoryListener listener;

    public GCMemoryBarrier(@Nonnull AtomicBoolean sufficientMemory,
            @Nonnull GCListener gcListener, long gcCount,
            @Nonnull SegmentGCOptions gcOptions) {
        this.sufficientMemory = sufficientMemory;
        this.gcListener = gcListener;
        this.gcOptions = gcOptions;
        this.gcCount = gcCount;

        MemoryPoolMXBean pool = null;
        int percentage = gcOptions.getMemoryThreshold();
        if (percentage > 0) {
            pool = getMemoryPool();
            if (pool == null) {
                gcListener
                        .warn("TarMK GC #{}: Unable to setup monitoring of available memory.",
                                gcCount);
            }
        }
        if (pool != null) {
            emitter = (NotificationEmitter) getMemoryMXBean();
            listener = new MemoryListener();
            emitter.addNotificationListener(listener, null, null);
            MemoryUsage usage = pool.getCollectionUsage();
            long maxMemory = usage.getMax();
            long required = (long) (maxMemory * percentage / 100);
            gcListener
                    .info("TarMK GC #{}: setting up a listener to cancel compaction if available memory on pool '{}' drops below {}%, {} ({} bytes).",
                            gcCount, pool.getName(), percentage,
                            humanReadableByteCount(required), required);

            long warningThreshold = maxMemory - required;
            long current = pool.getCollectionUsageThreshold();
            if (current > 0) {
                warningThreshold = Math.min(warningThreshold, current);
            }
            pool.setCollectionUsageThreshold(warningThreshold);
            checkMemory(usage);
        } else {
            emitter = null;
            listener = null;
            this.sufficientMemory.set(true);
        }
    }

    private static MemoryPoolMXBean getMemoryPool() {
        long maxSize = 0;
        MemoryPoolMXBean maxPool = null;
        for (MemoryPoolMXBean pool : getMemoryPoolMXBeans()) {
            if (HEAP == pool.getType()
                    && pool.isCollectionUsageThresholdSupported()) {
                // Get usage after a GC, which is more stable, if available
                long poolSize = pool.getCollectionUsage().getMax();
                // Keep the pool with biggest size, by default it should be Old Gen Space
                if (poolSize > maxSize) {
                    maxPool = pool;
                }
            }
        }
        return maxPool;
    }

    private void checkMemory(MemoryUsage usage) {
        int percentage = gcOptions.getMemoryThreshold();
        long maxMemory = usage.getMax();
        long usedMemory = usage.getUsed();
        long avail = maxMemory - usedMemory;
        long required = (long) (maxMemory * percentage / 100);
        if (avail <= required) {
            gcListener
                    .warn("TarMK GC #{}: canceling compaction because available memory level {} ({} bytes) is too low, expecting at least {} ({} bytes)",
                            gcCount, humanReadableByteCount(avail), avail,
                            humanReadableByteCount(required), required);
            sufficientMemory.set(false);
        } else {
            gcListener
                    .info("TarMK GC #{}: available memory level {} ({} bytes) is good, expecting at least {} ({} bytes)",
                            gcCount, humanReadableByteCount(avail), avail,
                            humanReadableByteCount(required), required);
            sufficientMemory.set(true);
        }
    }

    @Override
    public void close() throws IOException {
        if (emitter != null && listener != null) {
            try {
                emitter.removeNotificationListener(listener);
            } catch (ListenerNotFoundException e) {
                //
            }
        }
    }

    private class MemoryListener implements NotificationListener {
        public void handleNotification(Notification notification,
                Object handback) {
            if (notification
                    .getType()
                    .equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
                if (sufficientMemory.get()) {
                    CompositeData cd = (CompositeData) notification
                            .getUserData();
                    MemoryNotificationInfo info = MemoryNotificationInfo
                            .from(cd);
                    checkMemory(info.getUsage());
                }
            }
        }
    }
}
