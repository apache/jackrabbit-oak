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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.metric.util.StatsProviderUtil;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * {@link PropertyUpdateCallback} that records statistics about a certain index size (on disk) and number of documents.
 */
public class LuceneIndexStatsUpdateCallback implements PropertyUpdateCallback {

    private static final String NO_DOCS = "NO_DOCS";
    private static final String INDEX_SIZE = "INDEX_SIZE";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String indexPath;
    private final LuceneIndexMBean luceneIndexMBean;
    private final StatisticsProvider statisticsProvider;
    private final AsyncIndexesSizeStatsUpdate asyncIndexesSizeStatsUpdate;
    private final IndexingContext indexingContext;

    private final BiFunction<String, Map<String, String>, HistogramStats> histogram;
    private final StatsProviderUtil statsProviderUtil;

    LuceneIndexStatsUpdateCallback(String indexPath, @NotNull LuceneIndexMBean luceneIndexMBean,
                                   @NotNull StatisticsProvider statisticsProvider,
                                   AsyncIndexesSizeStatsUpdate asyncIndexesSizeStatsUpdate,
                                   IndexingContext indexingContext) {
        this.indexPath = indexPath;
        this.luceneIndexMBean = luceneIndexMBean;
        this.statisticsProvider = statisticsProvider;
        this.asyncIndexesSizeStatsUpdate = asyncIndexesSizeStatsUpdate;
        this.indexingContext = indexingContext;
        statsProviderUtil = new StatsProviderUtil(this.statisticsProvider);
        this.histogram = statsProviderUtil.getHistoStats();
    }

    @Override
    public void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd, @Nullable PropertyState before, @Nullable PropertyState after) {
        // do nothing
    }

    @Override
    public void done() {
        if (shouldUpdateStats()) {
            try {
                Map<String, String> labels = Collections.singletonMap("index", indexPath);
                long startTime = System.currentTimeMillis();
                int docCount = Integer.parseInt(luceneIndexMBean.getDocCount(indexPath));
                HistogramStats docCountHistogram = histogram.apply(NO_DOCS, labels);
                docCountHistogram.update(docCount);
                log.trace("{} stats updated, docCount {}, timeToUpdate {}", indexPath, docCount, System.currentTimeMillis() - startTime);
                long indexSize = Long.parseLong(luceneIndexMBean.getSize(indexPath));
                HistogramStats indexSizeHistogram = histogram.apply(INDEX_SIZE, labels);
                indexSizeHistogram.update(indexSize);
                long endTime = System.currentTimeMillis();
                asyncIndexesSizeStatsUpdate.setLastStatsUpdateTime(indexPath, endTime);
                log.debug("{} stats updated; docCount {}, size {}, timeToUpdate {}", indexPath, docCount, indexSize, endTime - startTime);
            } catch (IOException e) {
                log.warn("could not update no_docs/index_size stats for index at {}", indexPath, e);
            }
        }
    }

    private boolean shouldUpdateStats() {
        boolean timeToUpdate = false;
        if (indexingContext.isAsync()
                && asyncIndexesSizeStatsUpdate != null
                && asyncIndexesSizeStatsUpdate.getScheduleTimeInMillis() >= 0
                && isScheduled()) {
            timeToUpdate = true;
        }
        return timeToUpdate;
    }

    private boolean isScheduled() {
        long lastStatsUpdateTime = asyncIndexesSizeStatsUpdate.getLastStatsUpdateTime(indexPath);
        long defaultStatsUpdateTime = asyncIndexesSizeStatsUpdate.getScheduleTimeInMillis();
        return System.currentTimeMillis() > lastStatsUpdateTime + defaultStatsUpdateTime;
    }
}
