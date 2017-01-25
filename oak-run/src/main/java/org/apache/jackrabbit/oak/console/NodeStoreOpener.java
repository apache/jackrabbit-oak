/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.console;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.sql.DataSource;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.hybrid.DocumentQueue;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import com.google.common.util.concurrent.MoreExecutors;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

/**
 * A tool to open a node store from command line options
 */
public class NodeStoreOpener {
    
    private static final long MB = 1024 * 1024;    
    
    public static NodeStoreFixture open(OptionParser parser, boolean writeMode, String... args) throws Exception {
        OptionSpec<Integer> clusterId = parser.accepts("clusterId", "MongoMK clusterId")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);
        OptionSpec<Void> readWriteOption = parser.accepts("read-write", "connect to repository in read-write mode");
        OptionSpec<String> fdsPathSpec = parser.accepts("fds-path", "Path to FDS store").withOptionalArg().defaultsTo("");
        OptionSpec<Void> segment = parser.accepts("segment", "Use oak-segment instead of oak-segment-tar");
        OptionSpec<Void> help = parser.acceptsAll(asList("h", "?", "help"), "show help").forHelp();
    
        // RDB specific options
        OptionSpec<String> rdbjdbcuser = parser.accepts("rdbjdbcuser", "RDB JDBC user").withOptionalArg().defaultsTo("");
        OptionSpec<String> rdbjdbcpasswd = parser.accepts("rdbjdbcpasswd", "RDB JDBC password").withOptionalArg().defaultsTo("");
    
        OptionSpec<String> nonOption = parser.nonOptions("{<path-to-repository> | <mongodb-uri>}");
        OptionSpec<Void> disableBranchesSpec = parser.
                accepts("disableBranches", "disable branches");    
        OptionSpec<Integer> cacheSizeSpec = parser.
                accepts("cacheSize", "cache size").withRequiredArg().
                ofType(Integer.class).defaultsTo(0);         
    
        OptionSet options = parser.parse(args);
        List<String> nonOptions = nonOption.values(options);
    
        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }
    
        if (nonOptions.isEmpty()) {
            return new NodeStoreFixture() {
                @Override
                public void close() throws IOException {
                    // ignore
                }
                @Override
                public NodeStore getStore() {
                    return null;
                }
            };
        }
    
        BlobStore blobStore = null;
        String fdsPath = fdsPathSpec.value(options);
        if (!"".equals(fdsPath)) {
            File fdsDir = new File(fdsPath);
            if (fdsDir.exists()) {
                FileDataStore fds = new FileDataStore();
                fds.setPath(fdsDir.getAbsolutePath());
                fds.init(null);
    
                blobStore = new DataStoreBlobStore(fds);
            }
        }
        
        boolean readOnly = !writeMode && !options.has(readWriteOption);
    
        NodeStoreFixture fixture;
        String nodeStore = nonOptions.get(0);
        if (nodeStore.startsWith(MongoURI.MONGODB_PREFIX)) {
            MongoClientURI uri = new MongoClientURI(nodeStore);
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: " + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
    
            DocumentMK.Builder builder = new DocumentMK.Builder()
                    .setBlobStore(blobStore)
                    .setMongoDB(mongo.getDB()).
                    setClusterId(clusterId.value(options));
            if (readOnly) {
                builder.setReadOnlyMode();
            }
            DocumentNodeStore store = builder.getNodeStore();
            if (options.has(disableBranchesSpec)) {
                builder.disableBranches();
            }
            int cacheSize = cacheSizeSpec.value(options);
            if (cacheSize != 0) {
                builder.memoryCacheSize(cacheSize * MB);
            }            
            fixture = new MongoFixture(store);
        } else if (nodeStore.startsWith("jdbc")) {
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(nodeStore, rdbjdbcuser.value(options),
                    rdbjdbcpasswd.value(options));
            DocumentMK.Builder builder = new DocumentMK.Builder()
                    .setBlobStore(blobStore)
                    .setRDBConnection(ds).
                    setClusterId(clusterId.value(options));
            if (readOnly) {
                builder.setReadOnlyMode();
            }
            DocumentNodeStore store = builder.getNodeStore();
            if (options.has(disableBranchesSpec)) {
                builder.disableBranches();
            }
            int cacheSize = cacheSizeSpec.value(options);
            if (cacheSize != 0) {
                builder.memoryCacheSize(cacheSize * MB);
            }            
            fixture = new MongoFixture(store);
        } else if (options.has(segment)) {
            FileStore.Builder fsBuilder = FileStore.builder(new File(nodeStore))
                    .withMaxFileSize(256).withDefaultMemoryMapping();
            if (blobStore != null) {
                fsBuilder.withBlobStore(blobStore);
            }
            FileStore store;
            if (readOnly) {
                store = fsBuilder.buildReadOnly();
            } else {
                store = fsBuilder.build();
            }
            fixture = new SegmentFixture(store);
        } else {
            fixture = SegmentTarFixture.create(new File(nodeStore), readOnly, blobStore);
        }
        return fixture;
    }
    
    public static class MongoFixture implements NodeStoreFixture {
        private final DocumentNodeStore nodeStore;

        private MongoFixture(DocumentNodeStore nodeStore) {
            this.nodeStore = nodeStore;
        }

        @Override
        public NodeStore getStore() {
            return nodeStore;
        }

        @Override
        public void close() throws IOException {
            nodeStore.dispose();
        }
    }

    @Deprecated
    public static class SegmentFixture implements NodeStoreFixture {
        private final SegmentStore segmentStore;
        private final SegmentNodeStore nodeStore;

        private SegmentFixture(SegmentStore segmentStore) {
            this.segmentStore = segmentStore;
            this.nodeStore = SegmentNodeStore.builder(segmentStore).build();
        }

        @Override
        public NodeStore getStore() {
            return nodeStore;
        }

        @Override
        public void close() throws IOException {
            segmentStore.close();
        }
    }

    public static Session openSession(NodeStore nodeStore) throws RepositoryException {
        if (nodeStore == null) {
            return null;
        }
        StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
        Oak oak = new Oak(nodeStore).with(ManagementFactory.getPlatformMBeanServer());
        oak.getWhiteboard().register(StatisticsProvider.class, statisticsProvider, Collections.emptyMap());
        LuceneIndexProvider provider = NodeStoreOpener.createLuceneIndexProvider();
        oak.with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(NodeStoreOpener.createLuceneIndexEditorProvider());
        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    private static LuceneIndexEditorProvider createLuceneIndexEditorProvider() {
        LuceneIndexEditorProvider ep = new LuceneIndexEditorProvider();
        ScheduledExecutorService executorService = MoreExecutors.getExitingScheduledExecutorService(
                (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(5));
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        int queueSize = Integer.getInteger("queueSize", 1000);
        IndexTracker tracker = new IndexTracker();
        DocumentQueue queue = new DocumentQueue(queueSize, tracker, executorService, statsProvider);
        ep.setIndexingQueue(queue);
        return ep;
    }

    private static LuceneIndexProvider createLuceneIndexProvider() {
        return new LuceneIndexProvider();
    }
    
}
