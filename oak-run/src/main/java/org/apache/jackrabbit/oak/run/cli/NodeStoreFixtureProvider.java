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

package org.apache.jackrabbit.oak.run.cli;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Collections.emptyMap;

public class NodeStoreFixtureProvider {
    public static NodeStoreFixture create(Options options) throws Exception {
        return create(options, !options.getOptionBean(CommonOptions.class).isReadWrite());
    }

    public static NodeStoreFixture create(Options options, boolean readOnly) throws Exception {
        CommonOptions commonOpts = options.getOptionBean(CommonOptions.class);
        Whiteboard wb = options.getWhiteboard();
        Closer closer = Closer.create();
        BlobStoreFixture blobFixture = BlobStoreFixtureProvider.create(options);
        BlobStore blobStore = null;
        if (blobFixture != null) {
            blobStore = blobFixture.getBlobStore();
            closer.register(blobFixture);
        }

        StatisticsProvider statisticsProvider = createStatsProvider(options, wb, closer);
        wb.register(StatisticsProvider.class, statisticsProvider, emptyMap());

        NodeStore store;
        if (commonOpts.isMongo() || commonOpts.isRDB()) {
            store = DocumentFixtureProvider.configureDocumentMk(options, blobStore, wb, closer, readOnly);
        } else if (commonOpts.isOldSegment()) {
            store = SegmentFixtureProvider.create(options, blobStore, wb, closer, readOnly);
        } else {
            store = SegmentTarFixtureProvider.configureSegment(options, blobStore, wb, closer, readOnly);
        }

        return new SimpleNodeStoreFixture(store, blobStore, wb, closer);
    }

    private static StatisticsProvider createStatsProvider(Options options, Whiteboard wb, Closer closer) {
        if (options.getCommonOpts().isMetricsEnabled()) {
            ScheduledExecutorService executorService =
                    MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
            MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executorService);
            closer.register(statsProvider);
            closer.register(() -> reportMetrics(statsProvider));
            wb.register(MetricRegistry.class, statsProvider.getRegistry(), emptyMap());
            return statsProvider;
        }
        return StatisticsProvider.NOOP;
    }

    private static void reportMetrics(MetricStatisticsProvider statsProvider) {
        MetricRegistry metricRegistry = statsProvider.getRegistry();
        ConsoleReporter.forRegistry(metricRegistry)
                .outputTo(System.out)
                .filter((name, metric) -> {
                    if (metric instanceof Counting) {
                        //Only report non zero metrics
                        return ((Counting) metric).getCount() > 0;
                    }
                    return true;
                })
                .build()
                .report();
    }

    private static class SimpleNodeStoreFixture implements NodeStoreFixture {
        private final Closer closer;
        private final NodeStore nodeStore;
        private final BlobStore blobStore;
        private final Whiteboard whiteboard;

        private SimpleNodeStoreFixture(NodeStore nodeStore, BlobStore blobStore,
                                       Whiteboard whiteboard, Closer closer) {
            this.blobStore = blobStore;
            this.whiteboard = whiteboard;
            this.closer = closer;
            this.nodeStore = nodeStore;
        }

        @Override
        public NodeStore getStore() {
            return nodeStore;
        }

        @Override
        public BlobStore getBlobStore() {
            return blobStore;
        }

        @Override
        public Whiteboard getWhiteboard() {
            return whiteboard;
        }

        @Override
        public void close() throws IOException {
            closer.close();
        }
    }
}
