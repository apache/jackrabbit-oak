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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;


import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexFileSystemStatistics implements Runnable {

    private final StatisticsProvider statisticsProvider;
    private final IndexCopier indexCopier;
    private static final String LOCAL_INDEX_DIR_SIZE = "LOCAL_INDEX_DIR_SIZE";
    private final Logger log = LoggerFactory.getLogger(getClass());

    public LuceneIndexFileSystemStatistics(StatisticsProvider statsProvider, IndexCopier indexCopier) {
        this.statisticsProvider = statsProvider;
        this.indexCopier = indexCopier;
    }

    @Override
    public void run() {
        calculateLocalIndexDirSize();
    }

    private void calculateLocalIndexDirSize() {
        long localIndexDirSize = indexCopier.getLocalIndexDirSize();
        CounterStats indexDirectorySizeStats = statisticsProvider.getCounterStats(LOCAL_INDEX_DIR_SIZE, StatsOptions.DEFAULT);
        long deltaInSize = localIndexDirSize - indexDirectorySizeStats.getCount();
        if (deltaInSize != 0) {
            indexDirectorySizeStats.inc(deltaInSize);
            log.debug("index directory size stats updated; size {} delta {}", localIndexDirSize, deltaInSize);
        }
    }
}
