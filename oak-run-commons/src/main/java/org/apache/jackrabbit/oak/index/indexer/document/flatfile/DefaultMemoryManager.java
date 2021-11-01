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

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
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

    /**
     * Max memory to be used if jmx based memory monitoring is not available. This value is not considered if jmx based
     * monitoring is available.
     */
    private final long maxMemoryBytes;
    /**
     * When jmx based memory monitoring is available, this value indicates minimum memory which should be free/available for this
     * task to proceed.
     */
    private final long minMemoryBytes;
    private final AtomicLong memoryUsed;
    private final MemoryPoolMXBean pool;
    private final ConcurrentHashMap<String, MemoryManagerClient> clients;
    private final MemoryManager.Type type;
    private final Random random;

    public DefaultMemoryManager() {
        this(Integer.getInteger(OAK_INDEXER_MIN_MEMORY, 2) * ONE_GB,
                Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT) * ONE_GB);
    }

    public DefaultMemoryManager(long minMemoryInBytes, long maxMemoryInBytes) {
        pool = getMemoryPool();
        memoryUsed = new AtomicLong(0);
        clients = new ConcurrentHashMap<>();
        random =  ThreadLocalRandom.current();
        if (pool == null) {
            maxMemoryBytes = maxMemoryInBytes;
            minMemoryBytes = 0;
            type = Type.SELF_MANAGED;
            log.warn("Unable to setup monitoring of available memory. " +
                    "Would use configured maxMemory limit of {} GB", maxMemoryInBytes/ONE_GB);
        } else {
            maxMemoryBytes = 0;
            MemoryUsage usage = pool.getCollectionUsage();
            long maxAvailable = usage.getMax();
            minMemoryBytes = minMemoryInBytes < maxAvailable ? minMemoryInBytes : (long)(0.5*maxAvailable);
            if (minMemoryBytes != minMemoryInBytes) {
                log.warn("Provided minimum memory {} GB more than available memory ({})." +
                        "Overriding configuration and setting min memory to 50% of available memory ({}).",
                        minMemoryInBytes/ONE_GB, humanReadableByteCount(maxAvailable), humanReadableByteCount(minMemoryBytes));
            }
            type = Type.JMX_BASED;
            configureMemoryListener();
        }
        logFlags();
    }

    private void configureMemoryListener() {
        NotificationEmitter emitter = (NotificationEmitter) getMemoryMXBean();
        MemoryListener listener = new MemoryListener();
        emitter.addNotificationListener(listener, null, null);
        MemoryUsage usage = pool.getCollectionUsage();
        long maxMemory = usage.getMax();
        log.info("Setting up a listener to monitor pool '{}' and trigger batch save " +
                "if memory drop below {} GB (max {})", pool.getName(), minMemoryBytes/ONE_GB, humanReadableByteCount(maxMemory));
        pool.setCollectionUsageThreshold(minMemoryBytes);
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public boolean isMemoryLow() {
        if (type == Type.SELF_MANAGED) {
            return memoryUsed.get() > maxMemoryBytes;
        }
        return !sufficientMemory.get();
    }

    @Override
    public void changeMemoryUsedBy(long memory) {
        if (type != Type.SELF_MANAGED) {
            throw new UnsupportedOperationException("Not a self managed memory manager");
        }
        memoryUsed.addAndGet(memory);
    }

    @Override
    public Optional<String> registerClient(MemoryManagerClient client) {
        if (type != Type.JMX_BASED) {
            throw new UnsupportedOperationException("Not a JMX based memory manager");
        }
        if (!sufficientMemory.get()) {
            log.info("Can't register new client now. Not enough memory.");
            return Optional.empty();
        }
        int retryCount = 0;
        String registrationID = generateRegistrationID();
        while (retryCount < 5) {
            MemoryManagerClient oldClient = clients.putIfAbsent(registrationID, client);
            if (oldClient == null) {
                log.debug("Registered client with registration_id={}", registrationID);
                return Optional.of(registrationID);
            }
            retryCount++;
            registrationID = generateRegistrationID();
        }
        return Optional.empty();
    }

    @Override
    public boolean deregisterClient(String registrationID) {
        if (type != Type.JMX_BASED) {
            throw new UnsupportedOperationException("Not a JMX based memory manager");
        }
        boolean removed;
        if (clients.remove(registrationID) != null) {
            log.debug("Client with registration_id={} deregistered", registrationID);
            removed = true;
        } else {
            log.warn("No client found with registration_id={}", registrationID);
            removed = false;
        }
        return removed;
    }

    private String generateRegistrationID() {
        byte[] r = new byte[8];
        random.nextBytes(r);
        return Base64.encodeBase64String(r) + "-" + System.currentTimeMillis();
    }

    private long getAvailableMemory(MemoryUsage usage) {
        return usage.getMax() - usage.getUsed();
    }

    private void checkMemory(MemoryUsage usage) {
        long avail = getAvailableMemory(usage);
        if (avail > minMemoryBytes) {
            sufficientMemory.set(true);
            log.info("Available memory level {} is good.", humanReadableByteCount(avail));
        } else {
            boolean couldSet = sufficientMemory.compareAndSet(true, false);
            if (couldSet) {
                Phaser phaser = new Phaser();
                clients.forEach((r, c) -> c.memoryLow(phaser));
                log.info("Available memory level {} (required {}) is low. Enabling flag to trigger batch save",
                        humanReadableByteCount(avail), minMemoryBytes / ONE_GB);
                new Thread(() -> {
                    log.info("Waiting for all tasks to finish dumping their data");
                    phaser.awaitAdvance(phaser.getPhase());
                    log.info("All tasks have finished dumping their data");
                    sufficientMemory.set(true);
                }, "Wait-For-Dump").start();
            }
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
        if (type.equals(Type.JMX_BASED)) {
            log.info("Min heap memory (GB) to be required : {} ({})", minMemoryBytes / ONE_GB, OAK_INDEXER_MIN_MEMORY);
        }
        if (type.equals(Type.SELF_MANAGED)) {
            log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemoryBytes / ONE_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
        }
    }


}
