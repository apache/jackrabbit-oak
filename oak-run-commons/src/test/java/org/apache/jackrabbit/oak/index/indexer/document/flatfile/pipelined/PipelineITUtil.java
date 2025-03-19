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

import com.mongodb.MongoClientURI;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.TestUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class PipelineITUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineITUtil.class);

    private static final int LONG_PATH_TEST_LEVELS = 30;
    private static final String LONG_PATH_LEVEL_STRING = "Z12345678901234567890-Level_";

    private final static Clock.Virtual clock = new Clock.Virtual();
    static {
        TestUtils.setRevisionClock(clock);
    }


    static final List<String> EXPECTED_FFS = new ArrayList<>(List.of(
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

    static final PathFilter contentDamPathFilter = new PathFilter(List.of("/content/dam"), List.of());

    static {
        // Generate dynamically the entries expected for the long path tests
        StringBuilder path = new StringBuilder("/content/dam");
        for (int i = 0; i < LONG_PATH_TEST_LEVELS; i++) {
            path.append("/").append(LONG_PATH_LEVEL_STRING).append(i);
            EXPECTED_FFS.add(path + "|{}");
        }
    }

    static void createContent(NodeStore rwNodeStore) throws CommitFailedException {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("1000").child("12").setProperty("p1", "v100012");
        contentDamBuilder.child("2022").child("01").setProperty("p1", "v202201");
        contentDamBuilder.child("2022").child("01").child("01").setProperty("p1", "v20220101");
        contentDamBuilder.child("2022").child("02").setProperty("p1", "v202202");
        contentDamBuilder.child("2022").child("02").child("01").setProperty("p1", "v20220201");
        contentDamBuilder.child("2022").child("02").child("02").setProperty("p1", "v20220202");
        contentDamBuilder.child("2022").child("02").child("03").setProperty("p1", "v20220203");
        contentDamBuilder.child("2022").child("02").child("04").setProperty("p1", "v20220204");
        contentDamBuilder.child("2022").child("03").setProperty("p1", "v202203");
        contentDamBuilder.child("2022").child("04").setProperty("p1", "v202204");
        contentDamBuilder.child("2023").setProperty("p2", "v2023");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
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

    static void assertMetrics(MetricStatisticsProvider statsProvider) {
        // Check the statistics
        Set<String> metricsNames = statsProvider.getRegistry().getCounters().keySet();

        assertEquals(Set.of(
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_DURATION_SECONDS,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL,
                PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL_BYTES,
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

    static MongoTestBackend createNodeStore(boolean readOnly, MongoConnectionFactory connectionFactory, DocumentMKBuilderProvider builderProvider) {
        MongoConnection c = connectionFactory.getConnection();
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setMongoDB(c.getMongoClient(), c.getDBName());
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        builder.setAsyncDelay(1);
        builder.clock(clock);
        DocumentNodeStore documentNodeStore = builder.getNodeStore();
        return new MongoTestBackend(c.getMongoURI(), (MongoDocumentStore) builder.getDocumentStore(), documentNodeStore, c.getDatabase());
    }

    static MongoTestBackend createNodeStore(boolean readOnly, String mongoUri, DocumentMKBuilderProvider builderProvider) {
        MongoClientURI mongoClientUri = new MongoClientURI(mongoUri);
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setMongoDB(mongoUri, "oak", 0);
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        builder.setAsyncDelay(1);
        builder.clock(clock);
        DocumentNodeStore documentNodeStore = builder.getNodeStore();
        MongoDocumentStore mongoDocumentStore = (MongoDocumentStore) builder.getDocumentStore();
        // TODO: Resource not released
        MongoConnection c = new MongoConnection(mongoUri);
        return new MongoTestBackend(mongoClientUri, mongoDocumentStore, documentNodeStore, c.getDatabase());
    }


    static PipelinedStrategy createStrategy(MongoTestBackend backend, Predicate<String> pathPredicate, List<PathFilter> pathFilters, File storeDir) {
        Set<String> preferredPathElements = Set.of();
        RevisionVector rootRevision = backend.documentNodeStore.getRoot().getRootRevision();
        return new PipelinedStrategy(
                backend.mongoClientURI,
                backend.mongoDocumentStore,
                backend.documentNodeStore,
                rootRevision,
                preferredPathElements,
                new MemoryBlobStore(),
                storeDir,
                Compression.NONE,
                pathPredicate,
                pathFilters,
                null,
                StatisticsProvider.NOOP,
                IndexingReporter.NOOP);
    }
}
