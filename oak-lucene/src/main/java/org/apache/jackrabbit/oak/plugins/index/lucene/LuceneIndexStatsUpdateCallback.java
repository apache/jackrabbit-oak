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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link PropertyUpdateCallback} that records statistics about a certain index size (on disk) and number of documents.
 */
public class LuceneIndexStatsUpdateCallback implements PropertyUpdateCallback {

    private static final String NO_DOCS = "_NO_DOCS";
    private static final String INDEX_SIZE = "_INDEX_SIZE";
    private static final String LOCAL_INDEX_DIR_SIZE = "LOCAL_INDEX_DIR_SIZE";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String indexPath;
    private final LuceneIndexMBean luceneIndexMBean;
    private final StatisticsProvider statisticsProvider;
    private final IndexCopier indexCopier;

    LuceneIndexStatsUpdateCallback(String indexPath, @NotNull LuceneIndexMBean luceneIndexMBean,
                                   @NotNull StatisticsProvider statisticsProvider, @NotNull IndexCopier indexCopier) {
        this.indexPath = indexPath;
        this.luceneIndexMBean = luceneIndexMBean;
        this.statisticsProvider = statisticsProvider;
        this.indexCopier = indexCopier;
    }

    @Override
    public void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd, @Nullable PropertyState before, @Nullable PropertyState after) {
        // do nothing
    }

    @Override
    public void done() throws CommitFailedException {
        try {
            int docCount = Integer.parseInt(luceneIndexMBean.getDocCount(indexPath));
            HistogramStats docCountHistogram = statisticsProvider.getHistogram(indexPath + NO_DOCS, StatsOptions.METRICS_ONLY);
            docCountHistogram.update(docCount);

            long indexSize = Long.parseLong(luceneIndexMBean.getSize(indexPath));
            HistogramStats indexSizeHistogram = statisticsProvider.getHistogram(indexPath + INDEX_SIZE, StatsOptions.METRICS_ONLY);
            indexSizeHistogram.update(indexSize);

            long localIndexDirSize = indexCopier.getLocalIndexDirSize();

            CounterStats indexDirectorySizeStats = statisticsProvider.getCounterStats(LOCAL_INDEX_DIR_SIZE, StatsOptions.DEFAULT);
            long deltaInSize = localIndexDirSize - indexDirectorySizeStats.getCount();
            if (deltaInSize != 0) {
                indexDirectorySizeStats.inc(deltaInSize);
                log.debug("index directory size stats updated; size {} delta {}", localIndexDirSize, deltaInSize);
            }

            log.debug("{} stats updated; docCount {}, size {}", indexPath, docCount, indexSize);

        } catch (IOException e) {
            log.debug("could not update no_docs/index_size stats for index at {}", indexPath, e);
        }
    }
}
