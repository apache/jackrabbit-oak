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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An asynchronous buffer of the CacheAction objects. The buffer only accepts
 * {@link #MAX_SIZE} number of elements. If the queue is already full, the new
 * elements are dropped.
 */
public class CacheActionDispatcher implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CacheActionDispatcher.class);

    /**
     * Default maximum memory for the queue: 32 MB.
     */
    private static final long DEFAULT_MAX_MEMORY = 32 * 1024 * 1024;

    /**
     * The maximum length of the queue.
     */
    static final int MAX_SIZE = 16 * 1024;

    final BlockingQueue<CacheAction> queue = new ArrayBlockingQueue<>(MAX_SIZE);

    /**
     * The maximum memory for all cache actions currently in the queue.
     */
    private final long maxMemory;

    /**
     * The current memory usage of the cache actions in the queue.
     */
    private long memory = 0;

    /**
     * Monitor object for synchronization.
     */
    private final Object monitor = new Object();

    private volatile boolean isRunning = true;

    public CacheActionDispatcher() {
        this(DEFAULT_MAX_MEMORY);
    }

    CacheActionDispatcher(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                CacheAction action = queue.poll(10, TimeUnit.MILLISECONDS);
                if (action != null && isRunning) {
                    synchronized (monitor) {
                        memory -= action.getMemory();
                    }
                    action.execute();
                }
            } catch (InterruptedException e) {
                LOG.debug("Interrupted the queue.poll()", e);
            }
        }
    }

    /**
     * Stop the processing.
     */
    public void stop() {
        isRunning = false;
    }

    /**
     * Tries to add new action.
     *
     * @param action to be added
     */
    boolean add(CacheAction action) {
        int m = action.getMemory();
        synchronized (monitor) {
            // check if the queue reached memory limit and accepts action
            if (memory + m <= maxMemory && queue.offer(action)) {
                memory += m;
                return true;
            }
        }
        return false;
    }

    /**
     * Exposed for tests only.
     *
     * @return the current memory usage of the pending cache actions.
     */
    long getMemory() {
        return memory;
    }
}