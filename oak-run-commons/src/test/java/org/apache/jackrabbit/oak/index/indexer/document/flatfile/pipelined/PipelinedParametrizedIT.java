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

import com.codahale.metrics.Counter;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import joptsimple.internal.Strings;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.index.ConsoleIndexingReporter;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoDownloaderRegexUtils.LONG_PATH_ID_PATTERN;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class PipelinedParametrizedIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedParametrizedIT.class);
    private static final int LONG_PATH_TEST_LEVELS = 30;
    private static final String LONG_PATH_LEVEL_STRING = "Z12345678901234567890-Level_";
    private static ScheduledExecutorService executorService;
    private static final List<String> EXPECTED_FFS = new ArrayList<>(List.of(
            "/|{}",
            "/content|{}",
            "/content/dam|{}",
            "/content/dam/1000|{}",
            "/content/dam/1000/12|{\"p1\":\"v100012\"}",
            "/content/dam/2022|{}",
            "/content/dam/2022/01|{\"p1\":\"v202201\"}",
            "/content/dam/2022/01/01|{\"p1\":\"v20220101\"}",
            "/content/dam/2022/02|{\"p1\":\"v202202\"}",
            "/content/dam/2022/02/01|{\"p1\":\"v20220201\"}",
            "/content/dam/2022/02/02|{\"p1\":\"v20220202\"}",
            "/content/dam/2022/02/03|{\"p1\":\"v20220203\"}",
            "/content/dam/2022/02/04|{\"p1\":\"v20220204\"}",
            "/content/dam/2022/03|{\"p1\":\"v202203\"}",
            "/content/dam/2022/04|{\"p1\":\"v202204\"}",
            "/content/dam/2023|{\"p2\":\"v2023\"}",
            "/content/dam/2023/01|{\"p1\":\"v202301\"}",
            "/content/dam/2023/02|{}",
            "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}"
    ));


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

    private final boolean retryOnConnectionErrors;
    private final boolean regexPathFiltering;
    private final boolean parallelDump;

    @Parameterized.Parameters(name = "retryOnConnectionErrors={0},regexPathFiltering={1},parallelDump={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[]{true, true, true},
                new Object[]{true, true, false},
                new Object[]{true, false, true},
                new Object[]{true, false, false},
                new Object[]{false, true, true},
                new Object[]{false, true, false},
                new Object[]{false, false, true},
                new Object[]{false, false, false}
        );
    }

    public PipelinedParametrizedIT(boolean retryOnConnectionErrors, boolean regexPathFiltering, boolean parallelDump) {
        this.retryOnConnectionErrors = retryOnConnectionErrors;
        this.regexPathFiltering = regexPathFiltering;
        this.parallelDump = parallelDump;
    }

    @Before
    public void before() {
        LOG.info("retryOnConnectionErrors={},regexPathFiltering={},parallelDump={}",
                retryOnConnectionErrors, regexPathFiltering, parallelDump);

        MongoConnection c = connectionFactory.getConnection();
        if (c != null) {
            c.getDatabase().drop();
        }
        statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executorService);
        indexingReporter = new ConsoleIndexingReporter();
    }

    @After
    public void tear() {
        LOG.info("tear");
        MongoConnection c = connectionFactory.getConnection();
        if (c != null) {
            c.getDatabase().drop();
        }
        statsProvider.close();
        statsProvider = null;
        indexingReporter = null;
    }


    @Test
    public void testCreateFFS() throws IOException, CommitFailedException {
        LOG.info("testCreateFFS");
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, Boolean.toString(retryOnConnectionErrors));
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, Boolean.toString(regexPathFiltering));
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, Boolean.toString(parallelDump));

        Predicate<String> pathPredicate = s -> PipelineITUtil.contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        List<PathFilter> pathFilters = List.of(PipelineITUtil.contentDamPathFilter);

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> pathFilters)
            throws CommitFailedException, IOException {
        testSuccessfulDownload(pathPredicate, pathFilters, EXPECTED_FFS, false);
    }

    private long numberOfDocumentsOnMongo(MongoDatabase mongoDatabase) {
        return mongoDatabase.getCollection(org.apache.jackrabbit.oak.plugins.document.Collection.NODES.toString()).countDocuments();
    }

    private long numberOfFilteredDocuments(MongoDatabase mongoDatabase, String path) {
        String idRegex = "^[0-9]{1,3}:" + Pattern.quote(path);
        int parentNodes = Path.fromString(path).getDepth();
        long childNodes = mongoDatabase.getCollection(org.apache.jackrabbit.oak.plugins.document.Collection.NODES.toString())
                .countDocuments(Filters.or(
                        Filters.regex(NodeDocument.ID, idRegex),
                        Filters.regex(NodeDocument.ID, LONG_PATH_ID_PATTERN))
                );
        return parentNodes + childNodes;
    }


    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> pathFilters, List<String> expected, boolean ignoreLongPaths)
            throws CommitFailedException, IOException {
        try (MongoTestBackend rwStore = PipelineITUtil.createNodeStore(false, connectionFactory, builderProvider)) {
            PipelineITUtil.createContent(rwStore.documentNodeStore);
        }

        try (MongoTestBackend roStore = PipelineITUtil.createNodeStore(true, connectionFactory, builderProvider)) {
            PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, pathFilters);
            File file = pipelinedStrategy.createSortedStoreFile();
            SortedMap<String, Counter> counters = statsProvider.getRegistry().getCounters();

            long nDocumentsMatchingFilter = regexPathFiltering ?
                    numberOfFilteredDocuments(roStore.mongoDatabase, "/content/dam") :
                    numberOfDocumentsOnMongo(roStore.mongoDatabase);
            long actualDocumentsDownloaded = counters.get(PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL).getCount();
            long actualEntriesAccepted = counters.get(PipelinedMetrics.OAK_INDEXER_PIPELINED_ENTRIES_ACCEPTED_TOTAL).getCount();
            if (parallelDump) {
                // With parallel download, we may download more documents than the ones matching the filter. In the worst
                // case, we download every document 3x: one time for each of the ascending and descending threads, and
                // one more time for the catch-up at the end of the download
                assertTrue("Docs downloaded: " + actualDocumentsDownloaded + " must be between " + nDocumentsMatchingFilter + " and " + nDocumentsMatchingFilter * 3,
                        nDocumentsMatchingFilter <= actualDocumentsDownloaded && actualDocumentsDownloaded <= nDocumentsMatchingFilter * 3);
                assertTrue("Entries downloaded: " + actualEntriesAccepted + " must be between " + expected.size() + " and " + expected.size() * 3,
                        expected.size() <= actualEntriesAccepted && actualEntriesAccepted <= expected.size() * 3L);
            } else {
                assertEquals(nDocumentsMatchingFilter, actualDocumentsDownloaded);
                assertEquals(expected.size(), actualEntriesAccepted);
            }


            assertTrue(file.exists());
            List<String> result = Files.readAllLines(file.toPath());
            if (ignoreLongPaths) {
                // Remove the long paths from the result. The filter on Mongo is best-effort, it will download long path
                // documents, even if they do not match the includedPaths.
                result = result.stream()
                        .filter(s -> {
                            var name = s.split("\\|")[0];
                            return name.length() < Utils.PATH_LONG;
                        })
                        .collect(Collectors.toList());
            }
            assertEquals("Expected:\n" + Strings.join(expected, "\n") + "\nActual: " + Strings.join(result, "\n"), expected, result);
            PipelineITUtil.assertMetrics(statsProvider);
        }
    }

    private PipelinedStrategy createStrategy(MongoTestBackend backend, Predicate<String> pathPredicate, List<PathFilter> pathFilters) {
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
                pathFilters,
                null,
                statsProvider,
                indexingReporter);
    }
}
