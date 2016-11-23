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

package org.apache.jackrabbit.oak.plugins.index;

import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class TrackingCorruptIndexHandler implements CorruptIndexHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Clock clock = Clock.SIMPLE;
    private int indexerCycleCount;
    private long corruptIntervalMillis = TimeUnit.MINUTES.toMillis(30);
    private final Map<String, CorruptIndexInfo> indexes = Maps.newConcurrentMap();

    public Map<String, CorruptIndexInfo> getCorruptIndexData(String asyncName){
        if (corruptIntervalMillis <= 0){
            return Collections.emptyMap();
        }

        Map<String, CorruptIndexInfo> result = Maps.newHashMap();
        for (CorruptIndexInfo info : indexes.values()){
            if (asyncName.equals(info.asyncName) && info.isFailingSinceLongTime()){
                result.put(info.path, info);
            }
        }
        return result;
    }

    public Map<String, CorruptIndexInfo> getFailingIndexData(String asyncName){
        Map<String, CorruptIndexInfo> result = Maps.newHashMap();
        for (CorruptIndexInfo info : indexes.values()){
            if (asyncName.equals(info.asyncName)){
                result.put(info.path, info);
            }
        }
        return result;
    }

    public void markWorkingIndexes(Set<String> updatedIndexPaths) {
        indexerCycleCount++;
        for (String indexPath : updatedIndexPaths){
            CorruptIndexInfo info = indexes.remove(indexPath);
            if (info != null){
                log.info("Index at [{}] which was so far failing {} is now working again.", info.path, info.getStats());
            }
        }
    }


    //~--------------------------------< CorruptIndexHandler >

    @Override
    public void skippingCorruptIndex(String async, String indexPath, Calendar corruptSince) {
        getOrCreateInfo(async, indexPath).skippedIndexing(checkNotNull(corruptSince));
    }

    @Override
    public void indexUpdateFailed(String async, String indexPath, Exception e) {
        getOrCreateInfo(async, indexPath).addFailure(e);
    }

    //~---------------------------------< Setters >

    public void setCorruptInterval(long interval, TimeUnit unit) {
        this.corruptIntervalMillis = unit.toMillis(interval);
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    long getCorruptIntervalMillis() {
        return corruptIntervalMillis;
    }

    private long getTime(){
        return clock.getTime();
    }

    private synchronized CorruptIndexInfo getOrCreateInfo(String asyncName, String indexPath) {
        CorruptIndexInfo info = indexes.get(indexPath);
        if (info == null){
            info = new CorruptIndexInfo(asyncName, indexPath);
            indexes.put(indexPath, info);
        }
        return info;
    }

    public class CorruptIndexInfo {
        private final String asyncName;
        private final String path;
        private final int lastIndexerCycleCount = indexerCycleCount;
        private String exception = "";
        private int failureCount;
        private int skippedCount;
        private long corruptSince;

        CorruptIndexInfo(String asyncName, String path) {
            this.asyncName = asyncName;
            this.path = path;
            this.corruptSince = getTime();
        }

        void addFailure(Exception e){
            exception = Throwables.getStackTraceAsString(e);
            failureCount++;
        }

        void skippedIndexing(Calendar corruptSince){
            skippedCount++;
            this.corruptSince = corruptSince.getTimeInMillis();
        }

        public boolean isFailingSinceLongTime() {
            return getTime() - corruptSince > corruptIntervalMillis;
        }

        public String getStats() {
            return String.format("since %tc ,%d indexing cycles, failed %d times, skipped %d time",
                    corruptSince, getCycleCount(), failureCount, skippedCount);
        }

        public Calendar getCorruptSinceAsCal() {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(corruptSince);
            return cal;
        }

        public String getLastException() {
            return exception;
        }

        public int getSkippedCount() {
            return skippedCount;
        }

        public String getPath() {
            return path;
        }

        private int getCycleCount() {
            return indexerCycleCount - lastIndexerCycleCount;
        }
    }
}
