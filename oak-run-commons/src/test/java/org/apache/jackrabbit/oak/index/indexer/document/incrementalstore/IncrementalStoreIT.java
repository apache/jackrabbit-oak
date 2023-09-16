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
package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IncrementalStoreIT {
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

    private FileBlobStore fileBlobStore;

    @Before
    public void setup() throws IOException {
        fileBlobStore = new FileBlobStore(sortFolder.newFolder().getAbsolutePath());
    }

    @After
    public void tear() {
        MongoConnection c = connectionFactory.getConnection();
        c.getDatabase().drop();
    }

    /**
     * This test creates:
     * 1. base ffs at checkpoint1
     * 2. incremental ffs between checkpoint1 and checkpoint2
     * 3. base ffs at checkpoint2
     * 4. merge baseffs at checkpoint1 and incremental ffs between checkpoint1 and checkpoint2
     * 5. merged file is compared with base ffs at checkpoint2
     */
    @Test
    public void incrementalFFSTest() throws Exception {
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createBaseContent(rwStore.right);
        String initialCheckpoint = rwStore.right.checkpoint(3600000);
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam");
        PipelinedStrategy pipelinedStrategy = createPipelinedStrategy(roStore, pathPredicate, initialCheckpoint);

        File baseFFSAtCheckpoint1 = pipelinedStrategy.createSortedStoreFile();
        File metadataAtCheckpoint1 = pipelinedStrategy.createMetadataFile();
        assertTrue(baseFFSAtCheckpoint1.exists());
        assertTrue(metadataAtCheckpoint1.exists());
        assertEquals(Arrays.asList(new String[]{"/content/dam|{}",
                        "/content/dam/1000|{}",
                        "/content/dam/1000/12|{\"p1\":\"v100012\"}",
                        "/content/dam/2022|{}",
                        "/content/dam/2022/02|{\"p1\":\"v202202\"}",
                        "/content/dam/2023|{\"p2\":\"v2023\"}",
                        "/content/dam/2023/01|{\"p1\":\"v202301\"}",
                        "/content/dam/2023/02|{}",
                        "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}",
                        "/content/dam/2023/04|{}",
                        "/content/dam/2023/04/29|{\"p1\":\"v20230429\"}",
                        "/content/dam/2023/04/30|{\"p1\":\"v20230430\"}"

                }),
                Files.readAllLines(baseFFSAtCheckpoint1.toPath()));
        assertEquals(Arrays.asList("{\"checkpoint\":\"" + initialCheckpoint + "\",\"storeType\":\"FlatFileStore\"," +
                        "\"strategy\":\"" + pipelinedStrategy.getClass().getSimpleName() + "\",\"preferredPaths\":[],\"pathPredicate\":{}}"),
                Files.readAllLines(metadataAtCheckpoint1.toPath()));

        Path initialFilePath = Files.move(baseFFSAtCheckpoint1.toPath(), new File(baseFFSAtCheckpoint1.getParent(), "initial.json").toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.move(new File(baseFFSAtCheckpoint1.getParent() + "/sort-work-dir").toPath(), new File(baseFFSAtCheckpoint1.getParent(), "/initial-sort-work-dir").toPath(), StandardCopyOption.REPLACE_EXISTING);


        createIncrementalContent(rwStore.right);
        String finalCheckpoint = rwStore.right.checkpoint(3600000);
        Predicate<String> pathPredicate1 = s -> s.startsWith("/content/dam");
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore1 = createNodeStore(true);
        PipelinedStrategy pipelinedStrategy1 = createPipelinedStrategy(roStore1, pathPredicate1, finalCheckpoint);

        File baseFFSCheckpoint2 = pipelinedStrategy1.createSortedStoreFile();
        assertTrue(baseFFSCheckpoint2.exists());
        assertEquals(Arrays.asList(new String[]{"/content/dam|{}",
                        "/content/dam/1000|{}",
                        "/content/dam/1000/12|{\"p1\":\"v100012\",\"p2\":\"v100012\"}",
                        "/content/dam/2022|{}",
                        "/content/dam/2022/02|{\"p1\":\"v202202-new\"}",
                        "/content/dam/2023|{}",
                        "/content/dam/2023/02|{}",
                        "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}",
                        "/content/dam/2023/03|{\"p1\":\"v202301\"}",
                        "/content/dam/2023/04|{}",
                        "/content/dam/2023/04/29|{\"p1\":\"v20230429\"}",
                        "/content/dam/2023/04/30|{\"p1\":\"v20230430\"}",
                }),
                Files.readAllLines(baseFFSCheckpoint2.toPath()));
        Path finalFilePath = Files.move(baseFFSAtCheckpoint1.toPath(), new File(baseFFSAtCheckpoint1.getParent(),
                "final.json").toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.move(new File(baseFFSAtCheckpoint1.getParent() + "/sort-work-dir").toPath(),
                new File(baseFFSAtCheckpoint1.getParent(), "/final-sort-work-dir").toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        IncrementalFlatFileStoreStrategy incrementalFlatFileStoreStrategy = createIncrementalStrategy(roStore, pathPredicate, initialCheckpoint, finalCheckpoint);
        File incrementalFile = incrementalFlatFileStoreStrategy.createSortedStoreFile();
        File incrementalStoreMetadata = incrementalFlatFileStoreStrategy.createMetadataFile();
        assertTrue(baseFFSCheckpoint2.exists());
        assertEquals(Arrays.asList("{\"beforeCheckpoint\":\"" + initialCheckpoint + "\",\"afterCheckpoint\":\"" + finalCheckpoint + "\"," +
                        "\"storeType\":\"" + incrementalFlatFileStoreStrategy.getStoreType() + "\"," +
                        "\"strategy\":\"" + incrementalFlatFileStoreStrategy.getClass().getSimpleName() + "\"," +
                        "\"preferredPaths\":[],\"pathPredicate\":{}}"),
                Files.readAllLines(incrementalStoreMetadata.toPath()));

        File mergedFile = new File(incrementalFile.getParentFile(), "/merged.json");
        new MergeIncrementalFlatFileStore(Set.of(),
                new File(initialFilePath.toString()), incrementalFile, mergedFile, Compression.NONE).doMerge();

        assertEquals(Files.readAllLines(finalFilePath), Files.readAllLines(mergedFile.toPath()));
    }

    private PipelinedStrategy createPipelinedStrategy(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore,
                                                      Predicate<String> pathPredicate, String checkpoint) {
        DocumentNodeStore readOnlyNodeStore = roStore.right;
        MongoDocumentStore readOnlyMongoDocStore = roStore.left;

        Set<String> preferredPathElements = Set.of();
        RevisionVector rootRevision = RevisionVector.fromString(checkpoint);
        return new PipelinedStrategy(
                readOnlyMongoDocStore,
                readOnlyNodeStore,
                rootRevision,
                preferredPathElements,
                fileBlobStore,
                sortFolder.getRoot(),
                Compression.NONE,
                pathPredicate,
                Collections.emptyList(),
                checkpoint
        );
    }

    private IncrementalFlatFileStoreStrategy createIncrementalStrategy(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore,
                                                                       Predicate<String> pathPredicate, String initialCheckpoint, String finalCheckpoint) {
        DocumentNodeStore readOnlyNodeStore = roStore.right;
        MongoDocumentStore readOnlyMongoDocStore = roStore.left;

        Set<String> preferredPathElements = Set.of();
        NodeState initialNodeState = readOnlyNodeStore.retrieve(initialCheckpoint);
        NodeState finalNodeState = readOnlyNodeStore.retrieve(finalCheckpoint);
        return new IncrementalFlatFileStoreStrategy(
                readOnlyNodeStore, initialCheckpoint, finalCheckpoint, sortFolder.getRoot(), preferredPathElements,
                Compression.NONE, pathPredicate, new NodeStateEntryWriter(fileBlobStore));
    }

    private void createBaseContent(NodeStore rwNodeStore) throws CommitFailedException {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
        contentDamBuilder.child("2022").child("02").setProperty("p1", "v202202");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301");
        contentDamBuilder.child("1000").child("12").setProperty("p1", "v100012");
        contentDamBuilder.child("2023").setProperty("p2", "v2023");
        contentDamBuilder.child("2023").child("02").child("28").setProperty("p1", "v20230228");
        contentDamBuilder.child("2023").child("04").child("29").setProperty("p1", "v20230429");
        contentDamBuilder.child("2023").child("04").child("30").setProperty("p1", "v20230430");
        rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void createIncrementalContent(NodeStore rwNodeStore) throws CommitFailedException {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("2023").child("01").remove(); //deleted node
        contentDamBuilder.child("2023").child("03").setProperty("p1", "v202301"); //added node
        contentDamBuilder.child("2022").child("02").setProperty("p1", "v202202-new");// property updated
        contentDamBuilder.child("1000").child("12").setProperty("p2", "v100012"); // new property added
        contentDamBuilder.child("2023").removeProperty("p2");// property deleted
        contentDamBuilder.child("2023").child("02").child("28").setProperty("p1", "v20230228");
        contentDamBuilder.child("2023").child("04").child("29").setProperty("p1", "v20230429");
        contentDamBuilder.child("2023").child("04").child("30").setProperty("p1", "v20230430");
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
