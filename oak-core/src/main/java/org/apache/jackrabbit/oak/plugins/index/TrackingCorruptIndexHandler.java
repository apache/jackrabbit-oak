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

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class TrackingCorruptIndexHandler implements CorruptIndexHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Clock clock = Clock.SIMPLE;
    private long errorWarnIntervalMillis = TimeUnit.MINUTES.toMillis(15);
    private long indexerCycleCount;
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

    public boolean isFailing(String asyncName) {
        return !getFailingIndexData(asyncName).isEmpty();
    }

    //~--------------------------------< CorruptIndexHandler >

    @Override
    public boolean skippingCorruptIndex(String async, String indexPath, Calendar corruptSince) {
        CorruptIndexInfo info = getOrCreateInfo(async, indexPath);
        if (info.skippedIndexing(checkNotNull(corruptSince))) {
            log.warn("Ignoring corrupt index [{}] which has been marked as corrupt [{}]. This index " +
                            "MUST be reindexed for indexing to work properly", indexPath,
                    info.getStats());
            return true;
        }
        return false;
    }

    @Override
    public void indexUpdateFailed(String async, String indexPath, Exception e) {
        getOrCreateInfo(async, indexPath).addFailure(e);
    }

    //~---------------------------------< Setters >

    public void setCorruptInterval(long interval, TimeUnit unit) {
        this.corruptIntervalMillis = unit.toMillis(interval);
    }

    public void setErrorWarnInterval(long errorWarnInterval, TimeUnit unit) {
        this.errorWarnIntervalMillis = unit.toMillis(errorWarnInterval);
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    long getCorruptIntervalMillis() {
        return corruptIntervalMillis;
    }

    long getErrorWarnIntervalMillis() {
        return errorWarnIntervalMillis;
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
        private final long lastIndexerCycleCount = indexerCycleCount;
        private final Stopwatch watch = Stopwatch.createStarted(new ClockTicker(clock));
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

        boolean skippedIndexing(Calendar corruptSince){
            skippedCount++;
            this.corruptSince = corruptSince.getTimeInMillis();
            if (watch.elapsed(TimeUnit.MILLISECONDS) > errorWarnIntervalMillis) {
                watch.reset().start();
                return true;
            }
            return false;
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

        public boolean isMarkedAsCorrupt(){
            return skippedCount > 0;
        }

        public int getSkippedCount() {
            return skippedCount;
        }

        public String getPath() {
            return path;
        }

        private long getCycleCount() {
            return indexerCycleCount - lastIndexerCycleCount;
        }
    }

    //~-----------------------------------------------------< MBean Support >

    public TabularData getFailingIndexStats(String asyncName) {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(TrackingCorruptIndexHandler.class.getName(),
                    "Failing Index Stats", FailingIndexStats.TYPE, new String[]{"path"});
            tds = new TabularDataSupport(tt);
            Map<String, CorruptIndexInfo> infos = getFailingIndexData(asyncName);
            for (Map.Entry<String, CorruptIndexInfo> e : infos.entrySet()) {
                FailingIndexStats stats = new FailingIndexStats(e.getValue());
                tds.put(stats.toCompositeData());
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    private static class FailingIndexStats {
        static final String[] FIELD_NAMES = new String[]{
                "path",
                "stats",
                "markedCorrupt",
                "failingSince",
                "exception"
        };

        static final String[] FIELD_DESCRIPTIONS = new String[]{
                "Path",
                "Failure stats",
                "Marked as corrupt",
                "Failure start time",
                "Exception"
        };

        @SuppressWarnings("rawtypes")
        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.BOOLEAN,
                SimpleType.STRING,
                SimpleType.STRING,
        };

        static final CompositeType TYPE = createCompositeType();

        static CompositeType createCompositeType() {
            try {
                return new CompositeType(
                        FailingIndexStats.class.getName(),
                        "Composite data type for Failing Index statistics",
                        FailingIndexStats.FIELD_NAMES,
                        FailingIndexStats.FIELD_DESCRIPTIONS,
                        FailingIndexStats.FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }

        private final CorruptIndexInfo info;

        public FailingIndexStats(CorruptIndexInfo info){
            this.info = info;
        }

        CompositeDataSupport toCompositeData() {
            Object[] values = new Object[]{
                    info.path,
                    info.getStats(),
                    info.isMarkedAsCorrupt(),
                    String.format("%tc", info.getCorruptSinceAsCal().getTimeInMillis()),
                    info.getLastException(),
            };
            try {
                return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static class ClockTicker extends Ticker {
        private final Clock clock;

        public ClockTicker(Clock clock) {
            this.clock = clock;
        }

        @Override
        public long read() {
            return TimeUnit.MILLISECONDS.toNanos(clock.getTime());
        }
    }
}
