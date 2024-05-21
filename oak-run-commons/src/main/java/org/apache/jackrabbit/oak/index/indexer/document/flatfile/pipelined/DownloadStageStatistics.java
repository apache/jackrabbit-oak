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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import java.util.concurrent.atomic.LongAdder;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.MetricsUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregates statistics when downloading from Mongo with two threads
 */
public class DownloadStageStatistics {

    public static final Logger LOG = LoggerFactory.getLogger(DownloadStageStatistics.class);

    private final LongAdder totalEnqueueWaitTimeMillis = new LongAdder();
    private final LongAdder documentsDownloadedTotal = new LongAdder();
    private final LongAdder documentsDownloadedTotalBytes = new LongAdder();


    public long getTotalEnqueueWaitTimeMillis() {
        return totalEnqueueWaitTimeMillis.sum();
    }

    public long getDocumentsDownloadedTotal() {
        return documentsDownloadedTotal.sum();
    }

    public long getDocumentsDownloadedTotalBytes() {
        return documentsDownloadedTotalBytes.sum();
    }

    public void incrementTotalEnqueueWaitTimeMillis(long millis) {
        this.totalEnqueueWaitTimeMillis.add(millis);
    }

    public void incrementDocumentsDownloadedTotal() {
        this.documentsDownloadedTotal.increment();
    }

    public void incrementDocumentsDownloadedTotalBytes(long bytes) {
        this.documentsDownloadedTotalBytes.add(bytes);
    }

    @Override
    public String toString() {
        return MetricsFormatter.newBuilder()
                               .add("totalEnqueueWaitTimeMillis", getTotalEnqueueWaitTimeMillis())
                               .add("documentsDownloadedTotal", getDocumentsDownloadedTotal())
                               .add("documentsDownloadedTotalBytes",
                                   getDocumentsDownloadedTotalBytes())
                               .build();
    }

    public void publishStatistics(StatisticsProvider statisticsProvider, IndexingReporter reporter,
        long durationMillis) {
        LOG.info("Publishing download stage statistics");
        MetricsUtils.addMetric(statisticsProvider, reporter,
            PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_DURATION_SECONDS,
            durationMillis / 1000);
        MetricsUtils.addMetric(statisticsProvider, reporter,
            PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL,
            getDocumentsDownloadedTotal());
        MetricsUtils.addMetric(statisticsProvider, reporter,
            PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE,
            PipelinedUtils.toPercentage(getTotalEnqueueWaitTimeMillis(), durationMillis)
        );
        MetricsUtils.addMetricByteSize(statisticsProvider, reporter,
            PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL_BYTES,
            getDocumentsDownloadedTotalBytes());
    }

    public String formatStats(long durationMillis) {
        String enqueueingDelayPercentage = PipelinedUtils.formatAsPercentage(
            getTotalEnqueueWaitTimeMillis(), durationMillis);
        long durationSeconds = durationMillis / 1000;
        return MetricsFormatter.newBuilder()
                               .add("duration", FormattingUtils.formatToSeconds(durationSeconds))
                               .add("durationSeconds", durationSeconds)
                               .add("documentsDownloaded", getDocumentsDownloadedTotal())
                               .add("documentsDownloadedTotalBytes",
                                   getDocumentsDownloadedTotalBytes())
                               .add("dataDownloaded", IOUtils.humanReadableByteCountBin(
                                   getDocumentsDownloadedTotalBytes()))
                               .add("enqueueingDelayMillis", getTotalEnqueueWaitTimeMillis())
                               .add("enqueueingDelayPercentage", enqueueingDelayPercentage)
                               .build();
    }
}
