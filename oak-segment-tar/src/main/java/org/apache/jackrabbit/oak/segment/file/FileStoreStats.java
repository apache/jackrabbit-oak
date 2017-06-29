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

import static org.apache.jackrabbit.stats.TimeSeriesStatsUtil.asCompositeData;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.segment.file.tar.FileStoreMonitor;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;

public class FileStoreStats implements FileStoreStatsMBean, FileStoreMonitor {
    public static final String SEGMENT_REPO_SIZE = "SEGMENT_REPO_SIZE";
    public static final String SEGMENT_WRITES = "SEGMENT_WRITES";
    public static final String JOURNAL_WRITES = "JOURNAL_WRITES";
    
    private final StatisticsProvider statisticsProvider;
    private final FileStore store;
    private final MeterStats writeStats;
    private final CounterStats repoSize;
    private final MeterStats journalWriteStats;
    
    public FileStoreStats(StatisticsProvider statisticsProvider, FileStore store, long initialSize) {
        this.statisticsProvider = statisticsProvider;
        this.store = store;
        this.writeStats = statisticsProvider.getMeter(SEGMENT_WRITES, StatsOptions.DEFAULT);
        this.repoSize = statisticsProvider.getCounterStats(SEGMENT_REPO_SIZE, StatsOptions.DEFAULT);
        this.journalWriteStats = statisticsProvider.getMeter(JOURNAL_WRITES, StatsOptions.DEFAULT);
        repoSize.inc(initialSize);
    }

    public void init(long initialSize) {
        repoSize.inc(initialSize);
    }

    //~-----------------------------< FileStoreMonitor >

    @Override
    public void written(long delta) {
        writeStats.mark(delta);
        repoSize.inc(delta);
    }

    @Override
    public void reclaimed(long size) {
        repoSize.dec(size);
    }
    
    @Override
    public void flushed() {
        journalWriteStats.mark();
    }

    //~--------------------------------< FileStoreStatsMBean >

    @Override
    public long getApproximateSize() {
        return repoSize.getCount();
    }

    @Override
    public int getTarFileCount() {
        return store.readerCount() + 1; //1 for the writer
    }

    @Nonnull
    @Override
    public CompositeData getWriteStats() {
        return asCompositeData(getTimeSeries(SEGMENT_WRITES), SEGMENT_WRITES);
    }

    @Nonnull
    @Override
    public CompositeData getRepositorySize() {
        return asCompositeData(getTimeSeries(SEGMENT_REPO_SIZE), SEGMENT_REPO_SIZE);
    }

    @Override
    public String fileStoreInfoAsString() {
        return String.format("Segment store size : %s%n" +
                "Number of tar files : %d",
                IOUtils.humanReadableByteCount(getApproximateSize()),
                getTarFileCount());
    }
    
    @Override
    public long getJournalWriteStatsAsCount() {
        return journalWriteStats.getCount();
    }
    
    @Override
    public CompositeData getJournalWriteStatsAsCompositeData() {
        return asCompositeData(getTimeSeries(JOURNAL_WRITES), JOURNAL_WRITES);
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }
}
