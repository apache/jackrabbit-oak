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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.sql.DataSource;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;
import com.mongodb.MongoClientURI;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

public class NodeStoreFixtureProvider {
    private static final long MB = 1024 * 1024;

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
            store = configureDocumentMk(options, blobStore, statisticsProvider, closer, wb, readOnly);
        } else {
            store = configureSegment(options, blobStore, statisticsProvider, closer, readOnly);
        }

        return new SimpleNodeStoreFixture(store, blobStore, wb, closer);
    }

    private static NodeStore configureDocumentMk(Options options,
                                                 BlobStore blobStore,
                                                 StatisticsProvider statisticsProvider,
                                                 Closer closer,
                                                 Whiteboard wb, boolean readOnly) throws UnknownHostException {
        DocumentMK.Builder builder = new DocumentMK.Builder();

        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }

        DocumentNodeStoreOptions docStoreOpts = options.getOptionBean(DocumentNodeStoreOptions.class);

        builder.setClusterId(docStoreOpts.getClusterId());
        builder.setStatisticsProvider(statisticsProvider);
        if (readOnly) {
            builder.setReadOnlyMode();
        }

        int cacheSize = docStoreOpts.getCacheSize();
        if (cacheSize != 0) {
            builder.memoryCacheSize(cacheSize * MB);
        }

        if (docStoreOpts.disableBranchesSpec()) {
            builder.disableBranches();
        }

        CommonOptions commonOpts = options.getOptionBean(CommonOptions.class);

        if (docStoreOpts.isCacheDistributionDefined()){
            builder.memoryCacheDistribution(
                    docStoreOpts.getNodeCachePercentage(),
                    docStoreOpts.getPrevDocCachePercentage(),
                    docStoreOpts.getChildrenCachePercentage(),
                    docStoreOpts.getDiffCachePercentage()
            );
        }

        DocumentNodeStore dns;
        if (commonOpts.isMongo()) {
            MongoClientURI uri = new MongoClientURI(commonOpts.getStoreArg());
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: "
                        + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
            wb.register(MongoConnection.class, mongo, emptyMap());
            closer.register(mongo::close);
            builder.setMongoDB(mongo.getDB());
            dns = builder.getNodeStore();
            wb.register(MongoDocumentStore.class, (MongoDocumentStore) builder.getDocumentStore(), emptyMap());
        } else if (commonOpts.isRDB()) {
            RDBStoreOptions rdbOpts = options.getOptionBean(RDBStoreOptions.class);
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(commonOpts.getStoreArg(),
                    rdbOpts.getUser(), rdbOpts.getPassword());
            wb.register(DataSource.class, ds, emptyMap());
            builder.setRDBConnection(ds);
            dns = builder.getNodeStore();
            wb.register(RDBDocumentStore.class, (RDBDocumentStore) builder.getDocumentStore(), emptyMap());
        } else {
            throw new IllegalStateException("Unknown DocumentStore");
        }

        closer.register(() -> dns.dispose());

        return dns;
    }

    private static NodeStore configureSegment(Options options, BlobStore blobStore, StatisticsProvider statisticsProvider, Closer closer, boolean readOnly)
            throws IOException, InvalidFileStoreVersionException {

        String path = options.getOptionBean(CommonOptions.class).getStoreArg();
        FileStoreBuilder builder = fileStoreBuilder(new File(path)).withMaxFileSize(256);

        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        NodeStore nodeStore;
        if (readOnly) {
            ReadOnlyFileStore fileStore = builder
                    .withStatisticsProvider(statisticsProvider)
                    .buildReadOnly();
            closer.register(fileStore);
            nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        } else {
            FileStore fileStore = builder
                    .withStatisticsProvider(statisticsProvider)
                    .build();
            closer.register(fileStore);
            nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        }

        return nodeStore;
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
