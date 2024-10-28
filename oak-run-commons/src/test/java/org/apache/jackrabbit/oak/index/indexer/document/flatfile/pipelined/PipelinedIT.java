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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.index.ConsoleIndexingReporter;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelineITUtil.assertMetrics;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelineITUtil.contentDamPathFilter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_FILTERED_PATH;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PipelinedIT {
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
    private ConsoleIndexingReporter indexingReporter;

    @BeforeClass
    public static void setup() throws IOException {
        Assume.assumeTrue(MongoUtils.isAvailable());
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
        indexingReporter = new ConsoleIndexingReporter();
    }

    @After
    public void tear() {
        MongoConnection c = connectionFactory.getConnection();
        if (c != null) {
            c.getDatabase().drop();
        }
        statsProvider.close();
        statsProvider = null;
        indexingReporter = null;
    }

    @Test
    public void createFFS_mongoFiltering_include_excludes() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/content/dam/2023"), List.of("/content/dam/2023/02")));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/content/dam/2023|{\"p2\":\"v2023\"}",
                "/content/dam/2023/01|{\"p1\":\"v202301\"}",
                "/content/dam/2023/02|{}"
        ), true);
    }

    @Test
    public void createFFS_mongoFiltering_include_excludes2() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;

        // NOTE: If a path /a/b is in the excluded paths, the descendants of /a/b will not be downloaded but /a/b will
        // be downloaded. This is an intentional limitation of the logic to compute the Mongo filter which was done
        // to avoid the extra complexity of also filtering the root of the excluded tree. The transform stage would anyway
        // filter out these additional documents.
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/content/dam/1000", "/content/dam/2022"), List.of("/content/dam/2022/02", "/content/dam/2022/04")));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/content/dam/1000|{}",
                "/content/dam/1000/12|{\"p1\":\"v100012\"}",
                "/content/dam/2022|{}",
                "/content/dam/2022/01|{\"p1\":\"v202201\"}",
                "/content/dam/2022/01/01|{\"p1\":\"v20220101\"}",
                "/content/dam/2022/02|{\"p1\":\"v202202\"}",
                "/content/dam/2022/03|{\"p1\":\"v202203\"}",
                "/content/dam/2022/04|{\"p1\":\"v202204\"}"
        ), true);
    }


    @Test
    public void createFFS_mongoFiltering_include_excludes3() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;

        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/"), List.of("/content/dam", "/etc", "/home", "/jcr:system")));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/etc|{}",
                "/home|{}",
                "/jcr:system|{}"
        ), true);
    }

    @Test
    public void createFFS_mongoFiltering_include_excludes_retryOnConnectionErrors() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;

        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/"), List.of("/content/dam", "/etc", "/home", "/jcr:system")));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/etc|{}",
                "/home|{}",
                "/jcr:system|{}"
        ), true);
    }

    @Test
    public void createFFS_mongoFiltering_include_excludes4() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;

        List<PathFilter> pathFilters = List.of(
                new PathFilter(List.of("/content/dam/1000"), List.of()),
                new PathFilter(List.of("/content/dam/2022"), List.of("/content/dam/2022/01"))
        );

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/content/dam/1000|{}",
                "/content/dam/1000/12|{\"p1\":\"v100012\"}",
                "/content/dam/2022|{}",
                "/content/dam/2022/01|{\"p1\":\"v202201\"}",
                "/content/dam/2022/02|{\"p1\":\"v202202\"}",
                "/content/dam/2022/02/01|{\"p1\":\"v20220201\"}",
                "/content/dam/2022/02/02|{\"p1\":\"v20220202\"}",
                "/content/dam/2022/02/03|{\"p1\":\"v20220203\"}",
                "/content/dam/2022/02/04|{\"p1\":\"v20220204\"}",
                "/content/dam/2022/03|{\"p1\":\"v202203\"}",
                "/content/dam/2022/04|{\"p1\":\"v202204\"}"
        ), true);
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
        ), true);
    }

    @Test
    public void createFFS_filter_long_paths() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        // Create a filter on the node with the longest path
        String longestLine = PipelineITUtil.EXPECTED_FFS.stream().max(Comparator.comparingInt(String::length)).get();
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

        testSuccessfulDownload(pathPredicate, pathFilters, expected, false);
    }


    @Test
    public void createFFSCustomExcludePathsRegexRetryOnConnectionErrors() throws Exception {
        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        testPipelinedStrategy(Map.of(
                        // Filter all nodes ending in /metadata.xml or having a path section with ".*.jpg"
                        OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX, "/metadata.xml$|/.*.jpg/.*",
                        OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "true",
                        OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "false"
                ),
                this::buildNodeStoreForExcludedRegexTest,
                pathPredicate,
                null,
                excludedPathsRegexTestExpected);
    }

    @Test
    public void createFFSCustomExcludePathsRegexNoRetryOnConnectionError() throws Exception {
        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        testPipelinedStrategy(Map.of(
                        // Filter all nodes ending in /metadata.xml or having a path section with ".*.jpg"
                        OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX, "/metadata.xml$|/.*.jpg/.*",
                        OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false",
                        OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "false"
                ),
                this::buildNodeStoreForExcludedRegexTest,
                pathPredicate,
                null,
                excludedPathsRegexTestExpected);
    }

    @Test
    public void createFFSCustomExcludePathsRegexRetryOnConnectionErrorsRegexFiltering() throws Exception {
        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        testPipelinedStrategy(Map.of(
                        // Filter all nodes ending in /metadata.xml or having a path section with ".*.jpg"
                        OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX, "/metadata.xml$|/.*.jpg/.*",
                        OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "true",
                        OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true"
                ),
                this::buildNodeStoreForExcludedRegexTest,
                pathPredicate,
                List.of(contentDamPathFilter),
                excludedPathsRegexTestExpected);
    }

    @Test
    public void createFFSCustomExcludePathsRegexNoRetryOnConnectionErrorRegexFiltering() throws Exception {
        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        testPipelinedStrategy(Map.of(
                        // Filter all nodes ending in /metadata.xml or having a path section with ".*.jpg"
                        OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDE_ENTRIES_REGEX, "/metadata.xml$|/.*.jpg/.*",
                        OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false",
                        OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true"
                ),
                this::buildNodeStoreForExcludedRegexTest,
                pathPredicate,
                List.of(contentDamPathFilter),
                excludedPathsRegexTestExpected);
    }


    @Test
    public void createFFSFilterMongoDocuments() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_FILTERED_PATH, "/content/dam/2022");
        System.setProperty(OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "/01;/03;/02/28");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");

        Predicate<String> pathPredicate = s -> true;
        PathFilter pathFilter = new PathFilter(List.of("/content/dam/2022", "/content/dam/2023"), List.of());
        List<PathFilter> pathFilters = List.of(pathFilter);

        // The full list of nodes under /content/dam/2022 and /content/dam/2023 is:
        //  /content
        //  /content/dam
        //  /content/dam/2022
        //  /content/dam/2022/01
        //  /content/dam/2022/01/01
        //  /content/dam/2022/02
        //  /content/dam/2022/02/01
        //  /content/dam/2022/02/02
        //  /content/dam/2022/02/03
        //  /content/dam/2022/02/04
        //  /content/dam/2022/03
        //  /content/dam/2022/04
        //  /content/dam/2023
        //  /content/dam/2023/01
        //  /content/dam/2023/02
        //  /content/dam/2023/02/28
        // We are filtering nodes ending in /01 and /03 inside /content/dam/2022.
        // Nodes in /content/dam/2023 should not be filtered, even if they match the suffixes
        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/content/dam/2022|{}",
                "/content/dam/2022/02|{\"p1\":\"v202202\"}",
                "/content/dam/2022/02/02|{\"p1\":\"v20220202\"}",
                "/content/dam/2022/02/04|{\"p1\":\"v20220204\"}",
                "/content/dam/2022/04|{\"p1\":\"v202204\"}",
                "/content/dam/2023|{\"p2\":\"v2023\"}",
                "/content/dam/2023/01|{\"p1\":\"v202301\"}",
                "/content/dam/2023/02|{}",
                "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}"
        ), true);
    }

    private void buildNodeStoreForExcludedRegexTest(DocumentNodeStore rwNodeStore) {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("a.jpg").child("jcr:content").child("metadata.xml");
        contentDamBuilder.child("a.jpg").child("jcr:content").child("metadata.text");
        contentDamBuilder.child("image_a.png").child("jcr:content").child("metadata.text");
        contentDamBuilder.child("image_a.png").child("jcr:content").child("metadata.xml");
        try {
            rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
    }

    private final List<String> excludedPathsRegexTestExpected = List.of(
            "/|{}",
            "/content|{}",
            "/content/dam|{}",
            "/content/dam/a.jpg|{}",
            "/content/dam/image_a.png|{}",
            "/content/dam/image_a.png/jcr:content|{}",
            "/content/dam/image_a.png/jcr:content/metadata.text|{}"
    );

    private void testPipelinedStrategy(Map<String, String> settings,
                                       Consumer<DocumentNodeStore> contentBuilder,
                                       Predicate<String> pathPredicate,
                                       List<PathFilter> pathFilters,
                                       List<String> expected) throws IOException {
        settings.forEach(System::setProperty);

        try (MongoTestBackend rwStore = createNodeStore(false)) {
            DocumentNodeStore rwNodeStore = rwStore.documentNodeStore;
            contentBuilder.accept(rwNodeStore);
            MongoTestBackend roStore = createNodeStore(true);

            PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, pathFilters);
            File file = pipelinedStrategy.createSortedStoreFile();

            assertTrue(file.exists());
            assertEquals(expected, Files.readAllLines(file.toPath()));
            assertMetrics(statsProvider);
        }
    }

    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> pathFilters)
            throws CommitFailedException, IOException {
        testSuccessfulDownload(pathPredicate, pathFilters, PipelineITUtil.EXPECTED_FFS, false);
    }

    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> mongoRegexPathFilter, List<String> expected, boolean ignoreLongPaths)
            throws CommitFailedException, IOException {
        try (MongoTestBackend rwStore = createNodeStore(false)) {
            PipelineITUtil.createContent(rwStore.documentNodeStore);
        }

        try (MongoTestBackend roStore = createNodeStore(true)) {
            PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, mongoRegexPathFilter);
            File file = pipelinedStrategy.createSortedStoreFile();
            assertTrue(file.exists());
            List<String> result = Files.readAllLines(file.toPath());
            if (ignoreLongPaths) {
                // Remove the long paths from the result. The filter on Mongo is best-effort, it will download long path
                // documents, even if they do not match the includedPaths.
                result = result.stream()
                        .filter(s -> {
                            String name = s.split("\\|")[0];
                            return name.length() < Utils.PATH_LONG;
                        })
                        .collect(Collectors.toList());

            }
            assertEquals(expected, result);
            assertMetrics(statsProvider);
        }
    }

    @Test
    public void createFFS_pathPredicateDoesNotMatch() throws Exception {
        try (MongoTestBackend rwStore = createNodeStore(false)) {
            PipelineITUtil.createContent(rwStore.documentNodeStore);
        }

        try (MongoTestBackend roStore = createNodeStore(true)) {
            Predicate<String> pathPredicate = s -> s.startsWith("/content/dam/does-not-exist");
            PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, null);

            File file = pipelinedStrategy.createSortedStoreFile();

            assertTrue(file.exists());
            assertEquals("", Files.readString(file.toPath()));
        }
    }

    @Test
    public void createFFS_badNumberOfTransformThreads() throws CommitFailedException, IOException {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, "0");

        try (MongoTestBackend rwStore = createNodeStore(false)) {
            PipelineITUtil.createContent(rwStore.documentNodeStore);
        }

        try (MongoTestBackend roStore = createNodeStore(true)) {
            assertThrows("Invalid value for property " + PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS + ": 0. Must be > 0",
                    IllegalArgumentException.class,
                    () -> createStrategy(roStore)
            );
        }
    }

    @Test
    public void createFFS_badWorkingMemorySetting() throws CommitFailedException, IOException {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, "-1");

        try (MongoTestBackend rwStore = createNodeStore(false)) {
            PipelineITUtil.createContent(rwStore.documentNodeStore);
        }

        try (MongoTestBackend roStore = createNodeStore(true)) {
            assertThrows("Invalid value for property " + PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB + ": -1. Must be >= 0",
                    IllegalArgumentException.class,
                    () -> createStrategy(roStore)
            );
        }
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

        MongoTestBackend rwStore = createNodeStore(false);
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

        MongoTestBackend roStore = createNodeStore(true);
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, pathFilters);

        File file = pipelinedStrategy.createSortedStoreFile();
        assertTrue(file.exists());
        assertArrayEquals(expected.toArray(new String[0]), Files.readAllLines(file.toPath()).toArray(new String[0]));
        assertMetrics(statsProvider);
    }


    @Test
    public void createFFS_mongoFiltering_custom_excluded_paths_1() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS, "/etc,/home");

        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/"), List.of("/content/dam", "/etc", "/home", "/jcr:system")));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/etc|{}",
                "/home|{}",
                "/jcr:system|{}"
        ), true);
    }

    @Test
    public void createFFS_mongoFiltering_custom_excluded_paths_2() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS, "/etc,/home");

        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/"), List.of("/content/dam", "/jcr:system")));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/etc|{}",
                "/home|{}",
                "/jcr:system|{}"
        ), true);
    }

    @Test
    public void createFFS_mongoFiltering_custom_excluded_paths_3() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS, "/etc,/home,/content/dam,/jcr:system");

        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/"), List.of()));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/etc|{}",
                "/home|{}",
                "/jcr:system|{}"
        ), true);
    }

    @Test
    public void createFFSNoMatches() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS, "/etc,/home,/content/dam,/jcr:system");

        Predicate<String> pathPredicate = t -> true;
        List<PathFilter> mongoRegexPathFilters = List.of(new PathFilter(List.of("/doesnotexist"), List.of()));

        // For an included path of /foo, the / should not be included. But the mongo regex filter is only best effort,
        // and it will download the parents of all the included paths, even if they are empty. This is not a problem,
        // because the filter at the transform stage will remove these paths. This test has no filter at the transform
        // stage (pathPredicate is always true), so the / will be included in the result.
        testSuccessfulDownload(pathPredicate, mongoRegexPathFilters, List.of("/|{}"), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createFFS_mongoFiltering_custom_excluded_paths_cannot_exclude_root() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_CUSTOM_EXCLUDED_PATHS, "/etc,/");
        Predicate<String> pathPredicate = s -> true;
        List<PathFilter> pathFilters = List.of(new PathFilter(List.of("/"), List.of()));

        testSuccessfulDownload(pathPredicate, pathFilters, List.of(
                "/|{}",
                "/content|{}",
                "/content/dam|{}",
                "/etc|{}",
                "/home|{}",
                "/jcr:system|{}"
        ), true);
    }


    @Ignore("This test is for manual execution only. It allocates two byte buffers of 2GB each, which might exceed the memory available in the CI")
    public void createFFSWithPipelinedStrategy_veryLargeWorkingMemorySetting() throws Exception {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, "1");
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, "8000");

        try (MongoTestBackend rwStore = createNodeStore(false)) {
            PipelineITUtil.createContent(rwStore.documentNodeStore);
        }

        try (MongoTestBackend roStore = createNodeStore(true)) {
            Predicate<String> pathPredicate = s -> s.startsWith("/content/dam");
            PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, null);
            pipelinedStrategy.createSortedStoreFile();
        }
    }

    private MongoTestBackend createNodeStore(boolean b) {
        return PipelineITUtil.createNodeStore(b, connectionFactory, builderProvider);
    }

    private PipelinedStrategy createStrategy(MongoTestBackend roStore) {
        return createStrategy(roStore, s -> true, null);
    }

    private PipelinedStrategy createStrategy(MongoTestBackend backend, Predicate<String> pathPredicate, List<PathFilter> mongoRegexPathFilter) {
        Set<String> preferredPathElements = Set.of();
        RevisionVector rootRevision = backend.documentNodeStore.getRoot().getRootRevision();
        indexingReporter.setIndexNames(List.of("testIndex"));
        return new PipelinedStrategy(
                backend.mongoClientURI,
                backend.mongoDocumentStore,
                backend.documentNodeStore,
                rootRevision,
                preferredPathElements,
                new MemoryBlobStore(),
                sortFolder.getRoot(),
                Compression.NONE,
                pathPredicate,
                mongoRegexPathFilter,
                null,
                statsProvider,
                indexingReporter);
    }
}
