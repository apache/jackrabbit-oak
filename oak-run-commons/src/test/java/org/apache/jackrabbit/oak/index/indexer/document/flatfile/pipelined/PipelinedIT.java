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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
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
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class PipelinedIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedIT.class);

    @Rule
    public final MongoConnectionFactory connectionFactory = new MongoConnectionFactory();
    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
    @Rule
    public final TemporaryFolder sortFolder = new TemporaryFolder();

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void setup() throws IOException {
    }

    @After
    public void tear() {
        MongoConnection c = connectionFactory.getConnection();
        c.getDatabase().drop();
    }

    @Test
    public void createFFSWithPipelinedStrategy() throws Exception {
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createContent(rwStore.right);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore);

        File file = pipelinedStrategy.createSortedStoreFile();
        LOG.info("Created file: {}", file.getAbsolutePath());
        assertEquals("/content/dam|{}\n" +
                        "/content/dam/1000|{}\n" +
                        "/content/dam/1000/12|{\"p1\":\"v100012\"}\n" +
                        "/content/dam/2022|{}\n" +
                        "/content/dam/2022/02|{\"p1\":\"v202202\"}\n" +
                        "/content/dam/2023|{\"p2\":\"v2023\"}\n" +
                        "/content/dam/2023/01|{\"p1\":\"v202301\"}\n" +
                        "/content/dam/2023/02|{}\n" +
                        "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}\n",
                Files.readString(file.toPath()));
    }

    @Test
    public void createFFSWithPipelinedStrategy_badNumberOfTransformThreads() throws CommitFailedException {
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
    public void createFFSWithPipelinedStrategy_badWorkingMemorySetting() throws CommitFailedException {
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
        PipelinedStrategy pipelinedStrategy = createStrategy(roStore);

        pipelinedStrategy.createSortedStoreFile();
    }

    private PipelinedStrategy createStrategy(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore) {
        DocumentNodeStore readOnlyNodeStore = roStore.right;
        MongoDocumentStore readOnlyMongoDocStore = roStore.left;

        Set<String> preferredPathElements = Set.of();
        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam");
        RevisionVector rootRevision = readOnlyNodeStore.getRoot().getRootRevision();
        return new PipelinedStrategy(
                readOnlyMongoDocStore,
                readOnlyNodeStore,
                rootRevision,
                preferredPathElements,
                new MemoryBlobStore(),
                sortFolder.getRoot(),
                Compression.NONE,
                pathPredicate
        );
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
        rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

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
