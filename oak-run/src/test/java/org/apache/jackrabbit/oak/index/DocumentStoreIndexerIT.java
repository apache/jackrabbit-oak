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

package org.apache.jackrabbit.oak.index;

import com.codahale.metrics.Counter;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexerBase;
import org.apache.jackrabbit.oak.index.indexer.document.IndexerConfiguration;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundledTypesRegistry;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.index.importer.ClusterNodeStoreLock;
import org.apache.jackrabbit.oak.plugins.index.ConsoleIndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.OAK_INDEXER_USE_LZ4;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.OAK_INDEXER_USE_ZIP;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.createChild;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class DocumentStoreIndexerIT extends LuceneAbstractIndexCommandTest {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Rule
    public final TestRule restoreSystemProperties = new RestoreSystemProperties();

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    DocumentNodeStore dns;

    @Before
    public void setup() throws IOException {
        System.setProperty("java.io.tmpdir", temporaryFolder.newFolder("systemp").getAbsolutePath());
    }

    @After
    public void tear() {
        if (dns != null) {
            dns.dispose();
        }
    }

    @Test
    public void indexMongoRepo() throws Exception {
        dns = getNodeStore();
        fixture = new LuceneRepositoryFixture(temporaryFolder.getRoot(), dns);
        createTestData(false);
        String checkpoint = fixture.getNodeStore().checkpoint(TimeUnit.HOURS.toMillis(24));
        fixture.close();
        dns.dispose();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir=" + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--doc-traversal-mode",
                "--checkpoint=" + checkpoint,
                "--reindex",
                "--metrics",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                MongoUtils.URL
        };

        command.execute(args);

        File indexes = new File(outDir, IndexerSupport.LOCAL_INDEX_ROOT_DIR);
        assertTrue(indexes.exists());

        IndexRootDirectory idxRoot = new IndexRootDirectory(indexes);
        List<LocalIndexDir> idxDirs = idxRoot.getAllLocalIndexes();

        assertEquals(1, idxDirs.size());
    }

    @Test
    @Ignore("OAK-10495")
    public void parallelReindex() throws Exception {
        LOG.info("Starting parallelReindex");
        System.setProperty(IndexStoreUtils.OAK_INDEXER_USE_LZ4, "false");
        parallelReindexInternal();
        LOG.info("Finished parallelReindex");
    }

    @Test
    @Ignore("OAK-10495")
    public void parallelReindexWithLZ4() throws Exception {
        LOG.info("Starting parallelReindexWithLZ4");
        System.setProperty(OAK_INDEXER_USE_LZ4, "true");
        parallelReindexInternal();
        LOG.info("Finished parallelReindexWithLZ4");
    }

    /**
     * Test parallel indexing
     */
    private void parallelReindexInternal() throws Exception {
        System.setProperty("oak.indexer.minMemoryForWork", "1");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_PARALLEL_INDEX, "true");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_MIN_SPLIT_THRESHOLD, "0");
        System.setProperty(IndexerConfiguration.PROP_SPLIT_STORE_SIZE, "2");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_THREAD_POOL_SIZE, "2");
        System.setProperty(OAK_INDEXER_USE_ZIP, "true");

        DocumentNodeStore dns = getNodeStore();
        fixture = new LuceneRepositoryFixture(temporaryFolder.getRoot(), dns);

        createTestData("/testNodea/test/a", "foo", 40, "oak:Unstructured", true);
        createTestData("/testNodeb/test/b", "foo", 40, "oak:Unstructured", true);
        createTestData("/testNodec/test/c", "foo", 40, "oak:Unstructured", true);

        fixture.getAsyncIndexUpdate("async").run();
        int fooCount = getFooCount(fixture, "foo");
        assertEquals("async index wrong count", 120, fooCount);

        String checkpoint = fixture.getNodeStore().checkpoint(TimeUnit.HOURS.toMillis(24));
        fixture.close();

        IndexCommand command = new IndexCommand();
        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir=" + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--doc-traversal-mode",
                "--checkpoint=" + checkpoint,
                "--reindex",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                MongoUtils.URL
        };

        command.execute(args);

        File indexes = new File(outDir, IndexerSupport.LOCAL_INDEX_ROOT_DIR);
        assertTrue(indexes.exists());

        IndexRootDirectory idxRoot = new IndexRootDirectory(indexes);
        List<LocalIndexDir> idxDirs = idxRoot.getAllLocalIndexes();

        assertEquals(1, idxDirs.size());

        //~-----------------------------------------
        //Phase 2 - Import the indexes
        IndexCommand command2 = new IndexCommand();
        File indexDir = new File(outDir, IndexerSupport.LOCAL_INDEX_ROOT_DIR);
        String[] args3 = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-import-dir=" + indexDir.getAbsolutePath(),
                "--index-import",
                "--read-write",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                MongoUtils.URL
        };
        command2.execute(args3);

        //~-----------------------------------------
        //Phase 3 - Validate the import
        IndexRepositoryFixture fixture3 = new LuceneRepositoryFixture(temporaryFolder.getRoot(), dns);
        int foo3Count = getFooCount(fixture3, "foo");

        //new count should be same as previous
        assertEquals(fooCount, foo3Count);

        //Checkpoint must be released
        assertNull(fixture3.getNodeStore().retrieve(checkpoint));

        //Lock should also be released
        ClusterNodeStoreLock clusterLock = new ClusterNodeStoreLock(fixture3.getNodeStore());
        assertFalse(clusterLock.isLocked("async"));

        fixture3.close();
        dns.dispose();
    }

    private int getFooCount(IndexRepositoryFixture fixture, String propName) throws IOException, RepositoryException {
        Session session = fixture.getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        String explanation = getQueryPlan(fixture, "select * from [oak:Unstructured] where [" + propName + "] is not null");
        assertThat(explanation, containsString("/oak:index/fooIndex"));

        Query q = qm.createQuery("select * from [oak:Unstructured] where [foo] is not null", Query.JCR_SQL2);
        QueryResult result = q.execute();
        int size = Iterators.size(result.getNodes());
        session.logout();
        return size;
    }

    private static String getQueryPlan(IndexRepositoryFixture fixture, String query) throws RepositoryException, IOException {
        Session session = fixture.getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query explain = qm.createQuery("explain " + query, Query.JCR_SQL2);
        QueryResult explainResult = explain.execute();
        Row explainRow = explainResult.getRows().nextRow();
        String explanation = explainRow.getValue("plan").getString();
        session.logout();
        return explanation;
    }

    @Test
    public void indexMongoRepo_WithCompressionDisabled() throws Exception {
        System.setProperty(OAK_INDEXER_USE_ZIP, "false");
        indexMongoRepo();
        System.clearProperty(OAK_INDEXER_USE_ZIP);
    }

    @Test
    public void bundling() throws Exception {
        MongoConnection c1 = getConnection();
        DocumentNodeStoreBuilder<?> docBuilder = builderProvider.newBuilder()
                .setMongoDB(c1.getMongoClient(), c1.getDBName());
        DocumentNodeStore store = docBuilder.build();

        Whiteboard wb = new DefaultWhiteboard();
        MongoDocumentStore ds = (MongoDocumentStore) docBuilder.getDocumentStore();
        Registration r1 = wb.register(MongoDocumentStore.class, ds, emptyMap());
        wb.register(MongoClientURI.class, c1.getMongoURI(), emptyMap());
        wb.register(StatisticsProvider.class, StatisticsProvider.NOOP, emptyMap());
        wb.register(IndexingReporter.class, IndexingReporter.NOOP, emptyMap());
        Registration c1Registration = wb.register(MongoDatabase.class, c1.getDatabase(), emptyMap());

        configureIndex(store);
        configureBundling(store);

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder appNB = newNode("app:Asset");
        createChild(appNB,
                "jcr:content",
                "jcr:content/comments", //not bundled
                "jcr:content/metadata",
                "jcr:content/metadata/xmp", //not bundled
                "jcr:content/renditions", //includes all
                "jcr:content/renditions/original",
                "jcr:content/renditions/original/jcr:content"
        );
        builder.child("test").setChildNode("book.jpg", appNB.getNodeState());
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Check that bundling is working
        assertNull(getNodeDocument(ds, "/test/book.jpg/jcr:content"));
        assertNotNull(getNodeDocument(ds, "/test/book.jpg"));

        String checkpoint = store.checkpoint(100000);

        //Shut down this store and restart in readOnly mode
        store.dispose();
        r1.unregister();
        c1Registration.unregister();

        MongoConnection c2 = connectionFactory.getConnection();
        DocumentNodeStoreBuilder<?> docBuilderRO = builderProvider.newBuilder().setReadOnlyMode()
                .setMongoDB(c2.getMongoClient(), c2.getDBName());
        ds = (MongoDocumentStore) docBuilderRO.getDocumentStore();
        store = docBuilderRO.build();
        wb.register(MongoDocumentStore.class, ds, emptyMap());
        wb.register(MongoDatabase.class, c2.getDatabase(), emptyMap());

        ExtendedIndexHelper helper = new ExtendedIndexHelper(store, store.getBlobStore(), wb, temporaryFolder.newFolder(),
                temporaryFolder.newFolder(), List.of(TEST_INDEX_PATH));
        IndexerSupport support = new IndexerSupport(helper, checkpoint);

        CollectingIndexer testIndexer = new CollectingIndexer(p -> p.startsWith("/test"));
        DocumentStoreIndexer index = new DocumentStoreIndexer(helper, support) {
            @Override
            protected CompositeIndexer prepareIndexers(NodeStore nodeStore, NodeBuilder builder,
                                                       IndexingProgressReporter progressReporter) {
                return new CompositeIndexer(List.of(testIndexer));
            }
        };


        index.reindex();

        assertThat(testIndexer.paths, containsInAnyOrder(
                "/test",
                "/test/book.jpg",
                "/test/book.jpg/jcr:content",
                "/test/book.jpg/jcr:content/comments",
                "/test/book.jpg/jcr:content/metadata",
                "/test/book.jpg/jcr:content/metadata/xmp",
                "/test/book.jpg/jcr:content/renditions",
                "/test/book.jpg/jcr:content/renditions/original",
                "/test/book.jpg/jcr:content/renditions/original/jcr:content"
        ));

        store.dispose();

    }

    @Test
    public void metrics() throws Exception {
        MongoConnection mongoConnection = getConnection();
        DocumentNodeStoreBuilder<?> docBuilder = builderProvider.newBuilder()
                .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName());
        DocumentNodeStore store = docBuilder.build();

        Whiteboard wb = new DefaultWhiteboard();
        MongoDocumentStore ds = (MongoDocumentStore) docBuilder.getDocumentStore();
        Registration r1 = wb.register(MongoDocumentStore.class, ds, emptyMap());
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            MetricStatisticsProvider metricsStatisticsProvider = new MetricStatisticsProvider(null, executor);
            wb.register(StatisticsProvider.class, metricsStatisticsProvider, emptyMap());
            wb.register(IndexingReporter.class, new ConsoleIndexingReporter(), emptyMap());
            Registration c1Registration = wb.register(MongoDatabase.class, mongoConnection.getDatabase(), emptyMap());
            wb.register(MongoClientURI.class, mongoConnection.getMongoURI(), emptyMap());

            configureIndex(store);

            NodeBuilder builder = store.getRoot().builder();
            NodeBuilder appNB = newNode("app:Asset");
            createChild(appNB,
                    "jcr:content",
                    "jcr:content/comments",
                    "jcr:content/metadata",
                    "jcr:content/metadata/xmp",
                    "jcr:content/renditions",
                    "jcr:content/renditions/original",
                    "jcr:content/renditions/original/jcr:content"
            );
            builder.child("test").setChildNode("book.jpg", appNB.getNodeState());
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            String checkpoint = store.checkpoint(100000);

            //Shut down this store and restart in readOnly mode
            store.dispose();
            r1.unregister();
            c1Registration.unregister();

            MongoConnection c2 = connectionFactory.getConnection();
            DocumentNodeStoreBuilder<?> docBuilderRO = builderProvider.newBuilder().setReadOnlyMode()
                    .setMongoDB(c2.getMongoClient(), c2.getDBName());
            ds = (MongoDocumentStore) docBuilderRO.getDocumentStore();
            store = docBuilderRO.build();
            wb.register(MongoDocumentStore.class, ds, emptyMap());
            wb.register(MongoDatabase.class, c2.getDatabase(), emptyMap());

            ExtendedIndexHelper helper = new ExtendedIndexHelper(store, store.getBlobStore(), wb, temporaryFolder.newFolder(),
                    temporaryFolder.newFolder(), List.of(TEST_INDEX_PATH));
            IndexerSupport support = new IndexerSupport(helper, checkpoint);

            CollectingIndexer testIndexer = new CollectingIndexer(p -> p.startsWith("/test"));
            DocumentStoreIndexer index = new DocumentStoreIndexer(helper, support) {
                @Override
                protected CompositeIndexer prepareIndexers(NodeStore nodeStore, NodeBuilder builder,
                                                           IndexingProgressReporter progressReporter) {
                    return new CompositeIndexer(List.of(testIndexer));
                }
            };

            index.reindex();

            assertThat(testIndexer.paths, containsInAnyOrder(
                    "/test",
                    "/test/book.jpg",
                    "/test/book.jpg/jcr:content",
                    "/test/book.jpg/jcr:content/comments",
                    "/test/book.jpg/jcr:content/metadata",
                    "/test/book.jpg/jcr:content/metadata/xmp",
                    "/test/book.jpg/jcr:content/renditions",
                    "/test/book.jpg/jcr:content/renditions/original",
                    "/test/book.jpg/jcr:content/renditions/original/jcr:content"
            ));

            store.dispose();

            SortedMap<String, Counter> counters = metricsStatisticsProvider.getRegistry().getCounters();
            assertMetric(counters, DocumentStoreIndexerBase.METRIC_INDEXING_DURATION_SECONDS);
            assertMetric(counters, DocumentStoreIndexerBase.METRIC_FULL_INDEX_CREATION_DURATION_SECONDS);
        } finally {
            executor.shutdown();
        }
    }

    private void assertMetric(SortedMap<String, Counter> counters, String metricName) {
        Counter counter = counters.get(metricName);
        assertNotNull(counter);
        LOG.info("{} {}", metricName, counter.getCount());
    }

    @Test
    @Ignore("OAK-10495")
    public void testParallelIndexing() throws Exception {
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_PARALLEL_INDEX, "true");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_THREAD_POOL_SIZE, "2");
        bundling();
    }

    private void configureIndex(DocumentNodeStore store) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder idxb = childBuilder(builder, TEST_INDEX_PATH);

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder(idxb);
        defnb.indexRule("oak:Unstructured").property("foo").propertyIndex();
        defnb.build();

        merge(store, builder);
    }

    private DocumentNodeStore getNodeStore() {
        MongoConnection c = getConnection();
        return builderProvider.newBuilder()
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }

    private MongoConnection getConnection() {
        MongoConnection conn = connectionFactory.getConnection();
        assumeNotNull(conn);
        MongoUtils.dropCollections(conn.getDatabase());
        return conn;
    }

    private static void configureBundling(DocumentNodeStore store) throws CommitFailedException {
        NodeState registryState = BundledTypesRegistry.builder()
                .forType("app:Asset")
                .include("jcr:content")
                .include("jcr:content/metadata")
                .include("jcr:content/renditions")
                .include("jcr:content/renditions/**")
                .build();
        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        builder.getChildNode("jcr:system")
                .getChildNode(DOCUMENT_NODE_STORE)
                .getChildNode(BUNDLOR)
                .setChildNode("app:Asset", registryState.getChildNode("app:Asset"));
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeDocument getNodeDocument(DocumentStore ds, String path) {
        return ds.find(Collection.NODES, Utils.getIdFromPath(path));
    }

    private static NodeBuilder newNode(String typeName) {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static class CollectingIndexer implements NodeStateIndexer {
        private final Predicate<String> p;
        List<String> paths = new ArrayList<>();

        private CollectingIndexer(Predicate<String> p) {
            this.p = p;
        }

        @Override
        public boolean shouldInclude(String path) {
            return true;
        }

        @Override
        public boolean shouldInclude(NodeDocument doc) {
            return true;
        }

        @Override
        public boolean index(NodeStateEntry entry) {
            if (p.test(entry.getPath())) {
                paths.add(entry.getPath());
                return true;
            }
            return false;
        }

        @Override
        public boolean indexesRelativeNodes() {
            return false;
        }

        @Override
        public Set<String> getRelativeIndexedNodeNames() {
            return Collections.emptySet();
        }

        @Override
        public void close() {

        }
    }

}
