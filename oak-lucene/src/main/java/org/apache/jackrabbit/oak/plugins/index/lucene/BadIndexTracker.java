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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BadIndexTracker {
    /**
     * Time interval in millis after which a bad index would be accessed again
     * to check if it has been fixed
     */
    private static final long DEFAULT_RECHECK_INTERVAL = TimeUnit.MINUTES.toMillis(15);

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, BadIndexInfo> badIndexes = Maps.newConcurrentMap();
    private final long recheckIntervalMillis;
    private Ticker ticker = Ticker.systemTicker();
    private int indexerCycleCount;

    public BadIndexTracker() {
        this(DEFAULT_RECHECK_INTERVAL);
    }

    public BadIndexTracker(long recheckIntervalMillis) {
        this.recheckIntervalMillis = recheckIntervalMillis;
        log.info("Bad Index recheck interval set to {} seconds",
                TimeUnit.MILLISECONDS.toSeconds(recheckIntervalMillis));
    }

    public void markGoodIndexes(Set<String> updatedIndexPaths) {
        indexerCycleCount++;
        for (String indexPath : updatedIndexPaths) {
            markGoodIndex(indexPath);
        }
    }

    public void markGoodIndex(String indexPath) {
        BadIndexInfo info = badIndexes.remove(indexPath);
        if (info != null) {
            log.info("Index [{}] which was not working {} is found to be healthy again",
                    indexPath, info.getStats());
        }
    }

    public void markBadIndex(String path, Throwable e) {
        BadIndexInfo badIndex = badIndexes.get(path);
        if (badIndex == null) {
            badIndexes.put(path, new BadIndexInfo(path, e));
            log.error("Could not access the Lucene index at [{}]", path, e);
        } else {
            badIndex.failedAccess(e);
            log.error("Could not access the Lucene index at [{}] . {}",
                    path, badIndex.getStats(), e);
        }
    }

    public boolean isIgnoredBadIndex(String path) {
        BadIndexInfo badIdx = badIndexes.get(path);
        if (badIdx == null) {
            return false;
        }
        return !badIdx.tryAgain();
    }

    public Set<String> getIndexPaths() {
        return badIndexes.keySet();
    }

    public long getRecheckIntervalMillis() {
        return recheckIntervalMillis;
    }

    BadIndexInfo getInfo(String indexPath){
        return badIndexes.get(indexPath);
    }

    void setTicker(Ticker ticker) {
        this.ticker = ticker;
    }

    class BadIndexInfo {
        final String path;
        final int lastIndexerCycleCount = indexerCycleCount;
        private final long createdTime = TimeUnit.NANOSECONDS.toMillis(ticker.read());
        private final Stopwatch created = Stopwatch.createStarted(ticker);
        private final Stopwatch watch = Stopwatch.createStarted(ticker);
        private String exception;
        private int accessCount;
        private int failedAccessCount;

        public BadIndexInfo(String path, Throwable e) {
            this.path = path;
            this.exception = Throwables.getStackTraceAsString(e);
        }

        public boolean tryAgain() {
            accessCount++;

            if (watch.elapsed(TimeUnit.MILLISECONDS) > recheckIntervalMillis) {
                watch.reset().start();
                return true;
            }

            if (log.isDebugEnabled()) {
                log.debug("Ignoring index [{}] which is not working correctly {}", path, getStats());
            }
            return false;
        }

        public String getStats() {
            return String.format("since %s ,%d indexing cycles, accessed %d times",
                    created, getCycleCount(), accessCount);
        }

        public int getFailedAccessCount() {
            return failedAccessCount;
        }

        public int getAccessCount() {
            return accessCount;
        }

        public String getException() {
            return exception;
        }

        public long getCreatedTime() {
            return createdTime;
        }

        private int getCycleCount() {
            return indexerCycleCount - lastIndexerCycleCount;
        }

        public void failedAccess(Throwable e) {
            failedAccessCount++;
            exception = Throwables.getStackTraceAsString(e);
        }
    }

}
