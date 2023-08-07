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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
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
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PipelinedIT {
    @Rule
    public final MongoConnectionFactory connectionFactory = new MongoConnectionFactory();
    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
    @Rule
    public final TemporaryFolder sortFolder = new TemporaryFolder();

    private static final PathFilter contentDamPathFilter = new PathFilter(List.of("/content/dam"), List.of());

    private static final int LONG_PATH_TEST_LEVELS = 30;
    private static final String LONG_PATH_LEVEL_STRING = "Z12345678901234567890-Level_";

    @BeforeClass
    public static void setup() throws IOException {
        Assume.assumeTrue(MongoUtils.isAvailable());
        // Generate dynamically the entries expected for the long path tests
        StringBuilder path = new StringBuilder("/content/dam");
        for (int i = 0; i < LONG_PATH_TEST_LEVELS; i++) {
            path.append("/").append(LONG_PATH_LEVEL_STRING).append(i);
            EXPECTED_FFS.add(path + "|{}");
        }
    }

    @After
    public void tear() {
        MongoConnection c = connectionFactory.getConnection();
        if (c != null) {
            c.getDatabase().drop();
        }
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
    public void createFFS_noRetryOnMongoFailures_noMongoFiltering() throws Exception {
        System.setProperty(OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "false");
        System.setProperty(OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "false");

        Predicate<String> pathPredicate = s -> contentDamPathFilter.filter(s) != PathFilter.Result.EXCLUDE;
        List<PathFilter> pathFilters = null;

        testSuccessfulDownload(pathPredicate, pathFilters);
    }

    private void testSuccessfulDownload(Predicate<String> pathPredicate, List<PathFilter> pathFilters) throws CommitFailedException, IOException {
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createContent(rwStore.right);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);

        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, pathFilters);

        File file = pipelinedStrategy.createSortedStoreFile();
        assertTrue(file.exists());
        assertEquals(EXPECTED_FFS, Files.readAllLines(file.toPath()));
    }

    @Test
    public void createFFS_pathPredicateDoesNotMatch() throws Exception {
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createContent(rwStore.right);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam/does-not-exist");
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, null);

        File file = pipelinedStrategy.createSortedStoreFile();

        assertTrue(file.exists());
        assertEquals("", Files.readString(file.toPath()));
    }

    @Test
    public void createFFS_badNumberOfTransformThreads() throws CommitFailedException {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, "0");

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createContent(rwStore.right);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore);

        assertThrows("Invalid value for property " + PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS + ": 0. Must be > 0",
                IllegalArgumentException.class,
                pipelinedStrategy::createSortedStoreFile
        );
    }

    @Test
    public void createFFS_badWorkingMemorySetting() throws CommitFailedException {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, "-1");

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createContent(rwStore.right);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore);

        assertThrows("Invalid value for property " + PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB + ": -1. Must be >= 0",
                IllegalArgumentException.class,
                pipelinedStrategy::createSortedStoreFile
        );
    }

    @Ignore("This test is for manual execution only. It allocates two byte buffers of 2GB each, which might exceed the memory available in the CI")
    public void createFFSWithPipelinedStrategy_veryLargeWorkingMemorySetting() throws Exception {
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_TRANSFORM_THREADS, "1");
        System.setProperty(PipelinedStrategy.OAK_INDEXER_PIPELINED_WORKING_MEMORY_MB, "8000");

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createContent(rwStore.right);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam");
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore, pathPredicate, null);

        pipelinedStrategy.createSortedStoreFile();
    }

    private PipelinedStrategy createStrategy(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore) {
        return createStrategy(roStore, s -> true, null);
    }

    private PipelinedStrategy createStrategy(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore, Predicate<String> pathPredicate, List<PathFilter> pathFilters) {
        DocumentNodeStore readOnlyNodeStore = roStore.right;
        MongoDocumentStore readOnlyMongoDocStore = roStore.left;

        Set<String> preferredPathElements = Set.of();
        RevisionVector rootRevision = readOnlyNodeStore.getRoot().getRootRevision();
        return new PipelinedStrategy(
                readOnlyMongoDocStore,
                readOnlyNodeStore,
                rootRevision,
                preferredPathElements,
                new MemoryBlobStore(),
                sortFolder.getRoot(),
                Compression.NONE,
                pathPredicate,
                pathFilters);
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

    private ImmutablePair<MongoDocumentStore, DocumentNodeStore> createNodeStore(boolean readOnly) {
        MongoConnection c = connectionFactory.getConnection();
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setMongoDB(c.getMongoClient(), c.getDBName());
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        builder.setAsyncDelay(1);
        DocumentNodeStore documentNodeStore = builder.getNodeStore();
        return new ImmutablePair<>((MongoDocumentStore) builder.getDocumentStore(), documentNodeStore);
    }
}
