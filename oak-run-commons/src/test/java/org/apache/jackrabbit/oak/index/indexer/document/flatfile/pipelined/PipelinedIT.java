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

import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PipelinedIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedIT.class);
    private static final PathFilter contentDamPathFilter = new PathFilter(List.of("/content/dam"), List.of());
    private static final int LONG_PATH_TEST_LEVELS = 30;
    private static final String LONG_PATH_LEVEL_STRING = "Z12345678901234567890-Level_";

    private static ScheduledExecutorService executorService;
    @Rule
    public final MongoConnectionFactory connectionFactory = new MongoConnectionFactory();
    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
    @Rule
    public final TemporaryFolder sortFolder = new TemporaryFolder();


    private MetricStatisticsProvider statsProvider;

    @BeforeClass
    public static void setup() throws IOException {
        Assume.assumeTrue(MongoUtils.isAvailable());
        // Generate dynamically the entries expected for the long path tests
        StringBuilder path = new StringBuilder("/content/dam");
        for (int i = 0; i < LONG_PATH_TEST_LEVELS; i++) {
            path.append("/").append(LONG_PATH_LEVEL_STRING).append(i);
            EXPECTED_FFS.add(path + "|{}");
        }
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterClass
    public static void teardown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Before
    public void before() {
        MongoConnection c = connectionFactory.getConnection();
        if (c != null) {
            c.getDatabase().drop();
        }
        statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executorService);
    }

    @After
    public void tear() {
        MongoConnection c = connectionFactory.getConnection();
        if (c != null) {
            c.getDatabase().drop();
        }
        statsProvider.close();
        statsProvider = null;
    }

    @Test
    public void createFFS_retryOnMongoFailures_noMongoFiltering() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "false");

        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        List<PathFilter> pathFilters = null;

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    @Test
    public void createFFS_retryOnMongoFailures_mongoFiltering() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(contentDamPathFilter);

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    @Test
    public void createFFS_noRetryOnMongoFailures_mongoFiltering() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(contentDamPathFilter);

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    @Test
    public void createFFS_mongoFiltering_multipleIndexes() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;
        PathFilter pathFilter = new PathFilter(List.of("/content/dam/1000", "/content/dam/2023", "/content/dam/2023/01"), List.of());
        List<PathFilter> pathFilters = List.of(pathFilter);

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/content/dam/1000|{}",
                "/content/dam/1000/12|{\"p1\":\"v100012\"}",
                "/content/dam/2023|{\"p2\":\"v2023\"}",
                "/content/dam/2023/01|{\"p1\":\"v202301\"}",
                "/content/dam/2023/02|{}",
                "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}"
        ));
    }

    @Test
    public void createFFS_noRetryOnMongoFailures_noMongoFiltering() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "false");

        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/content/dam"), List.of()));

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    @Test
    public void createFFS_filter_long_paths() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        // Create a filter on the node with the longest path
        String longestLine = EXPECTED_FFS.stream().max(Comparator.comparingInt(String::length)).get();
        String longestPath = longestLine.substring(0, longestLine.lastIndexOf("|"));
        String parent = PathUtils.getParentPath(longestPath);
        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of(parent), List.of()));

        // The results should contain all the parents of the node with the longest path
        ArrayList<String> expected = new ArrayList<>();
        expected.add(longestPath + "|{}");
        while (true) {
            expected.add(parent + "|{}");
            if (parent.equals("/")) {
                break;
            }
            parent = PathUtils.getParentPath(parent);
        }
        // The list above has the longest paths first, reverse it to match the order in the FFS
        Collections.reverse(expected);

        testSuccessfulDownload(pathPredicate, pathFilters, expected);
    }

    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> pathFilters)
            throws CommitFailedException, IOException {
        testSuccessfulDownload(pathPredicate, pathFilters, EXPECTED_FFS);
    }

    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> pathFilters, List<String> expected)
            throws CommitFailedException, IOException {
        Backend rwStore = createNodeStore(false);
        createContent(rwStore.documentNodeStore);

        Backend roStore = createNodeStore(true);

        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, pathFilters);

        File file = pipelinedStrategy.createSortedStoreFile();
        assertTrue(file.exists());
        assertEquals(expected, Files.readAllLines(file.toPath()));
        assertMetrics();
    }

    private void assertMetrics() {
        // Check the statistics
        Set<String> metricsNames = statsProvider.getRegistry().getCounters().keySet();

        assertEquals(Set.of(
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_DURATION_SECONDS,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_TRAVERSED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_SPLIT_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_ACCEPTED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_ACCEPTED_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_REJECTED_EMPTY_NODE_STATE_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_TRAVERSED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_HIDDEN_PATHS_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_REJECTED_PATH_FILTERED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_EXTRACTED_ENTRIES_TOTAL_BYTES,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_CREATE_SORT_ARRAY_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_SORT_ARRAY_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_WRITE_TO_DISK_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FLAT_FILE_STORE_SIZE_BYTES,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_DURATION_SECONDS
        ), metricsNames);

        String pipelinedMetrics = statsProvider.getRegistry()
                .getCounters()
                .entrySet().stream()
                .map(e -> e.getKey() + " " + e.getValue().getCount())
                .collect(Collectors.joining("\n"));
        LOG.info("Metrics\n{}", pipelinedMetrics);
    }

    @Test
    public void createFFS_pathPredicateDoesNotMatch() throws Exception {
        Backend rwStore = createNodeStore(false);
        createContent(rwStore.documentNodeStore);

        Backend roStore = createNodeStore(true);
        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam/does-not-exist");
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, null);

        File file = pipelinedStrategy.createSortedStoreFile();

        assertTrue(file.exists());
        assertEquals("", Files.readString(file.toPath()));
    }

    @Test
    public void createFFS_badNumberOfTransformThreads() throws CommitFailedException {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, "0");

        Backend rwStore = createNodeStore(false);
        createContent(rwStore.documentNodeStore);

        Backend roStore = createNodeStore(true);

        assertThrows("Invalid value for property " + PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS + ": 0. Must be > 0",
                IllegalArgumentException.class,
                () -> createStrategy(roStore)
        );
    }

    @Test
    public void createFFS_badWorkingMemorySetting() throws CommitFailedException {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, "-1");

        Backend rwStore = createNodeStore(false);
        createContent(rwStore.documentNodeStore);

        Backend roStore = createNodeStore(true);

        assertThrows("Invalid value for property " + PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB + ": -1. Must be >= 0",
                IllegalArgumentException.class,
                () -> createStrategy(roStore)
        );
    }

    @Test
    public void createFFS_smallNumberOfDocsPerBatch() throws Exception {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_MAX_NUMBER_OF_DOCUMENTS, "2");

        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        List<PathFilter> pathFilters = null;

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    @Test
    public void createFFS_largeMongoDocuments() throws Exception {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_MONGO_DOC_BATCH_MAX_SIZE_MB, "1");
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_MONGO_DOC_QUEUE_RESERVED_MEMORY_MB, "32");

        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        List<PathFilter> pathFilters = null;

        Backend rwStore = createNodeStore(false);
        @NotNull NodeBuilder rootBuilder = rwStore.documentNodeStore.getRoot().builder();
        // This property does not fit in the reserved memory, but must still be processed without errors
        String longString = RandomStringUtils.random((int) (10 * FileUtils.ONE_MB), true, true);
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("2021").child("01").setProperty("p1", "v202101");
        contentDamBuilder.child("2022").child("01").setProperty("p1", longString);
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
        rwStore.documentNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        List<String> expected = List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/content/dam/2021|{}",
                "/content/dam/2021/01|{\"p1\":\"v202101\"}",
                "/content/dam/2022|{}",
                "/content/dam/2022/01|{\"p1\":\"" + longString + "\"}",
                "/content/dam/2023|{}",
                "/content/dam/2023/01|{\"p1\":\"v202301\"}"
        );

        Backend roStore = createNodeStore(true);
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, pathFilters);

        File file = pipelinedStrategy.createSortedStoreFile();
        assertTrue(file.exists());
        assertArrayEquals(expected.toArray(new String[0]), Files.readAllLines(file.toPath()).toArray(new String[0]));
        assertMetrics();
    }


    @Ignore("This test is for manual execution only. It allocates two byte buffers of 2GB each, which might exceed the memory available in the CI")
    public void createFFSWithPipelinedStrategy_veryLargeWorkingMemorySetting() throws Exception {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, "1");
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, "8000");

        Backend rwStore = createNodeStore(false);
        createContent(rwStore.documentNodeStore);

        Backend roStore = createNodeStore(true);
        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam");
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, null);

        pipelinedStrategy.createSortedStoreFile();
    }

    private PipelinedStrategy createStrategy(Backend roStore) {
        return createStrategy(roStore, s -> true, null);
    }

    private PipelinedStrategy createStrategy(Backend backend, Predicate<String> pathPredicate, List<PathFilter> pathFilters) {
        Set<String> preferredPathElements = Set.of();
        RevisionVector rootRevision = backend.documentNodeStore.getRoot().getRootRevision();
        return new PipelinedStrategy(
                backend.mongoDocumentStore,
                backend.mongoDatabase,
                backend.documentNodeStore,
                rootRevision,
                preferredPathElements,
                new MemoryBlobStore(),
                sortFolder.getRoot(),
                Compression.NONE,
                pathPredicate,
                pathFilters,
                null,
                statsProvider);
    }

    private void createContent(NodeStore rwNodeStore) throws CommitFailedException {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
        contentDamBuilder.child("2022").child("02").setProperty("p1", "v202202");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
        contentDamBuilder.child("1000").child("12").setProperty("p1", "v100012");
        contentDamBuilder.child("2023").setProperty("p2", "v2023");
        contentDamBuilder.child("2023").child("02").child("28").setProperty("p1", "v20230228");

        // Node with very long name
        @NotNull NodeBuilder node = contentDamBuilder;
        for (int i = 0; i < LONG_PATH_TEST_LEVELS; i++) {
            node = node.child(LONG_PATH_LEVEL_STRING + i);
        }

        // Other subtrees, to exercise filtering
        rootBuilder.child("jcr:system").child("jcr:versionStorage")
                .child("42").child("41").child("1.0").child("jcr:frozenNode").child("nodes").child("node0");
        rootBuilder.child("home").child("users").child("system").child("cq:services").child("internal")
                .child("dam").child("foobar").child("rep:principalPolicy").child("entry2")
                .child("rep:restrictions");
        rootBuilder.child("etc").child("scaffolding").child("jcr:content").child("cq:dialog")
                .child("content").child("items").child("tabs").child("items").child("basic")
                .child("items");

        rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static final List<String> EXPECTED_FFS = new ArrayList<>(List.of(
            "/|{}",
            "/content|{}",
            "/content/dam|{}",
            "/content/dam/1000|{}",
            "/content/dam/1000/12|{\"p1\":\"v100012\"}",
            "/content/dam/2022|{}",
            "/content/dam/2022/02|{\"p1\":\"v202202\"}",
            "/content/dam/2023|{\"p2\":\"v2023\"}",
            "/content/dam/2023/01|{\"p1\":\"v202301\"}",
            "/content/dam/2023/02|{}",
            "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}"
    ));

    private Backend createNodeStore(boolean readOnly) {
        MongoConnection c = connectionFactory.getConnection();
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setMongoDB(c.getMongoClient(), c.getDBName());
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        builder.setAsyncDelay(1);
        DocumentNodeStore documentNodeStore = builder.getNodeStore();
        return new Backend((MongoDocumentStore) builder.getDocumentStore(), documentNodeStore, c.getDatabase());
    }

    static class Backend {
        final MongoDocumentStore mongoDocumentStore;
        final DocumentNodeStore documentNodeStore;
        final MongoDatabase mongoDatabase;

        public Backend(MongoDocumentStore mongoDocumentStore, DocumentNodeStore documentNodeStore, MongoDatabase mongoDatabase) {
            this.mongoDocumentStore = mongoDocumentStore;
            this.documentNodeStore = documentNodeStore;
            this.mongoDatabase = mongoDatabase;
        }
    }
}
