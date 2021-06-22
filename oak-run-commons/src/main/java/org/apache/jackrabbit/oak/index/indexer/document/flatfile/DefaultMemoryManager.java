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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.management.ManagementFactory.getMemoryMXBean;
import static java.lang.management.ManagementFactory.getMemoryPoolMXBeans;
import static java.lang.management.MemoryType.HEAP;
import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;

public class DefaultMemoryManager implements MemoryManager {

    private static final String OAK_INDEXER_MIN_MEMORY = "oak.indexer.minMemoryForWork";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean sufficientMemory = new AtomicBoolean(true);

    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private final long minMemory = Integer.getInteger(OAK_INDEXER_MIN_MEMORY, 2);
    private final long maxMemoryBytes = maxMemory * ONE_GB;
    private final long minMemoryBytes = minMemory * ONE_GB;
    private final boolean useMaxMemory;
    private final AtomicLong memoryUsed;

    public DefaultMemoryManager() {
        MemoryPoolMXBean pool = getMemoryPool();
        memoryUsed = new AtomicLong(0);
        if (pool == null) {
            log.warn("Unable to setup monitoring of available memory. " +
                    "Would use configured maxMemory limit of {} GB", maxMemory);
            useMaxMemory = true;
            return;
        }
        useMaxMemory = false;
        configureMemoryListener(pool);
    }

    private void configureMemoryListener(MemoryPoolMXBean pool) {
        NotificationEmitter emitter = (NotificationEmitter) getMemoryMXBean();
        MemoryListener listener = new MemoryListener();
        emitter.addNotificationListener(listener, null, null);
        MemoryUsage usage = pool.getCollectionUsage();
        long maxMemory = usage.getMax();
        long warningThreshold = minMemory * ONE_GB;
        if (warningThreshold > maxMemory) {
            log.warn("Configured minimum memory {} GB more than available memory ({})." +
                    "Overriding configuration accordingly.", minMemory, humanReadableByteCount(maxMemory));
            warningThreshold = maxMemory;
        }
        log.info("Setting up a listener to monitor pool '{}' and trigger batch save " +
                "if memory drop below {} GB (max {})", pool.getName(), minMemory, humanReadableByteCount(maxMemory));
        pool.setCollectionUsageThreshold(warningThreshold);
        checkMemory(usage);
    }

    public boolean isMemoryLow() {
        if (useMaxMemory){
            return memoryUsed.get() > maxMemoryBytes;
        }
        return !sufficientMemory.get();
    }

    public void updateMemoryUsed(long memory) {
        memoryUsed.addAndGet(memory);
    }

    private void checkMemory(MemoryUsage usage) {
        long maxMemory = usage.getMax();
        long usedMemory = usage.getUsed();
        long avail = maxMemory - usedMemory;
        if (avail > minMemoryBytes) {
            sufficientMemory.set(true);
            log.info("Available memory level {} is good.", humanReadableByteCount(avail));
        } else {
            sufficientMemory.set(false);
            log.info("Available memory level {} (required {}) is low. Enabling flag to trigger batch save",
                    humanReadableByteCount(avail), minMemory);
        }
    }

    //Taken from GCMemoryBarrier
    private class MemoryListener implements NotificationListener {
        @Override
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

    private void logFlags() {
        log.info("Min heap memory (GB) to be required : {} ({})", minMemory, OAK_INDEXER_MIN_MEMORY);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }


}
