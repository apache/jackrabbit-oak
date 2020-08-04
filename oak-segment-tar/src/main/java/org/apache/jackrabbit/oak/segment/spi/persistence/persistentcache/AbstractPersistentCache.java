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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractPersistentCache implements PersistentCache, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractPersistentCache.class);

    public static final int THREADS = Integer.getInteger("oak.segment.cache.threads", 10);

    protected ExecutorService executor;
    protected AtomicLong cacheSize = new AtomicLong(0);
    protected PersistentCache nextCache;
    protected final HashSet<String> writesPending;

    public AbstractPersistentCache() {
        executor = Executors.newFixedThreadPool(THREADS);
        writesPending = new HashSet<>();
    }

    public PersistentCache linkWith(PersistentCache nextCache) {
        this.nextCache = nextCache;
        return nextCache;
    }

    @Override
    public void close() {
        try {
            executor.shutdown();
            if (executor.awaitTermination(60, SECONDS)) {
                logger.debug("The persistent cache scheduler was successfully shut down");
            } else {
                logger.warn("The persistent cache scheduler takes too long to shut down");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupt while shutting down the persistent cache scheduler", e);
            currentThread().interrupt();
        }
    }

    public int getWritesPending() {
        synchronized (writesPending) {
            return writesPending.size();
        }
    }

    protected boolean lockSegmentWrite(String segmentId) {
        synchronized (writesPending) {
            return writesPending.add(segmentId);
        }
    }

    protected void unlockSegmentWrite(String segmentId) {
        synchronized (writesPending) {
            writesPending.remove(segmentId);
        }
    }
}
