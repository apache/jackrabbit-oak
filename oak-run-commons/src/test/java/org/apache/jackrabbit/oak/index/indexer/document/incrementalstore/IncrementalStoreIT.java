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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.LZ4Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SimpleNodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadata;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadataOperatorImpl;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    /**
     * maximum temporary files external sort will create.
     */
    static final int DEFAULTMAXTEMPFILES = 1024;

    /**
     * Defines the default maximum memory to be used while sorting (8 MB)
     */
    static final long DEFAULT_MAX_MEM_BYTES = 8388608L;


    private Compression algorithm = Compression.GZIP;

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

    @Test
    public void testWithNoCompression() throws Exception {
        algorithm = Compression.NONE;
        incrementalFFSTest();
    }

    @Test
    public void testWithGzipCompression() throws Exception {
        algorithm = Compression.GZIP;
        incrementalFFSTest();
    }

    @Test
    public void testWithLz4Compression() throws Exception {
        algorithm = new LZ4Compression();
        incrementalFFSTest();
    }

    /**
     * This test creates:
     * 1. base ffs at checkpoint1
     * 2. incremental ffs between checkpoint1 and checkpoint2
     * 3. base ffs at checkpoint2
     * 4. merge baseffs at checkpoint1 and incremental ffs between checkpoint1 and checkpoint2
     * 5. merged file is compared with base ffs at checkpoint2
     *
     * @return
     */

    private Path createFFS(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore, Predicate<String> pathPredicate, String checkpoint, String identifier, String[] nodeStateAtCheckpoint) throws IOException {
        PipelinedStrategy pipelinedStrategy = createPipelinedStrategy(roStore, pathPredicate, checkpoint);

        File baseFFSAtCheckpoint1 = pipelinedStrategy.createSortedStoreFile();
        File metadataAtCheckpoint1 = pipelinedStrategy.createMetadataFile();
        assertTrue(baseFFSAtCheckpoint1.exists());
        assertTrue(metadataAtCheckpoint1.exists());
        try (BufferedReader bufferedReaderBaseFFSAtCheckpoint1 = IndexStoreUtils.createReader(baseFFSAtCheckpoint1, algorithm);
             BufferedReader bufferedReaderMetadataAtCheckpoint1 = IndexStoreUtils.createReader(metadataAtCheckpoint1, algorithm);
        ) {
            assertEquals(Arrays.asList(nodeStateAtCheckpoint),
                    bufferedReaderBaseFFSAtCheckpoint1.lines().collect(Collectors.toList()));

            assertEquals(List.of("{\"checkpoint\":\"" + checkpoint + "\",\"storeType\":\"FlatFileStore\"," +
                            "\"strategy\":\"" + pipelinedStrategy.getClass().getSimpleName() + "\",\"preferredPaths\":[]}"),
                    bufferedReaderMetadataAtCheckpoint1.lines().collect(Collectors.toList()));
        }

        Path initialFilePath = Files.move(baseFFSAtCheckpoint1.toPath(),
                new File(baseFFSAtCheckpoint1.getParent(), algorithm.addSuffix(identifier + ".json")).toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Path initialMetadataFilePath = Files.move(metadataAtCheckpoint1.toPath(),
                new File(baseFFSAtCheckpoint1.getParent(), algorithm.addSuffix(identifier + ".json.metadata")).toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Files.move(new File(baseFFSAtCheckpoint1.getParent() + "/sort-work-dir").toPath(),
                new File(baseFFSAtCheckpoint1.getParent(), "/" + identifier + "-sort-work-dir").toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        return initialFilePath;

    }

    private MergeIncrementalFlatFileStore createMergedFile(Path initialFfsPath, File incrementalFile, File mergedFile) throws IOException {
        MergeIncrementalFlatFileStore mergedStore = new MergeIncrementalFlatFileStore(Set.of(),
                new File(initialFfsPath.toString()), incrementalFile, mergedFile, algorithm);
        mergedStore.doMerge();
        return mergedStore;
    }

    public void incrementalFFSTest() throws Exception {
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> rwStore = createNodeStore(false);
        createBaseContent(rwStore.right);
        String initialCheckpoint = rwStore.right.checkpoint(3600000);
        ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore = createNodeStore(true);
        Predicate<String> pathPredicate = s -> true;

        Path initialFfsPath = createFFS(roStore, pathPredicate, initialCheckpoint, "initial", getNodeStateAtCheckpoint1());

        createIncrementalContent(rwStore.right);
        String finalCheckpoint = rwStore.right.checkpoint(3600000);

        Path finalFfsPath = createFFS(roStore, pathPredicate, finalCheckpoint, "final", getNodeStateAtCheckpoint2());


        File incrementalFile = createIncrementalStore(initialCheckpoint, roStore, pathPredicate, finalCheckpoint);
        File mergedFile = new File(incrementalFile.getParentFile(), algorithm.addSuffix("/merged.json"));
        assertTrue(mergedFile.createNewFile());
        MergeIncrementalFlatFileStore mergedStore = createMergedFile(initialFfsPath, incrementalFile, mergedFile);

        compareFinalFFSAndMergedFFS(finalFfsPath, mergedFile, mergedStore);

        File finalFFSForIndexing = createFFSFromBaseFFSUsingPreferredPathElementsAndFilterPredicate(mergedFile);
        assertEquals(List.of(getNodeStatesWithPrefferedPathsAndPathPredicatesOverBaseFFSAfterMerging()),
                IndexStoreUtils.createReader(finalFFSForIndexing, algorithm).lines().collect(Collectors.toList()));
    }

    private File createFFSFromBaseFFSUsingPreferredPathElementsAndFilterPredicate(File mergedFile) throws IOException {
        File finalFFSForIndexing = sortFolder.newFile();

        Function<String, NodeStateHolder> stringToType = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
        Function<NodeStateHolder, String> typeToString = holder -> holder == null ? null : holder.getLine();
        Set<String> preferredPathElements = Set.of("29", "04", "30");
        PathElementComparator pathComparator = new PathElementComparator(preferredPathElements);
        Comparator<NodeStateHolder> comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());

        Predicate<NodeStateHolder> filterPredicate = t -> NodeStateEntryWriter.getPath(t.getLine()).startsWith("/content/dam");
        List<File> files = ExternalSort.sortInBatch(mergedFile, comparator, DEFAULTMAXTEMPFILES, DEFAULT_MAX_MEM_BYTES, StandardCharsets.UTF_8,
                sortFolder.newFolder("filtered"), true, 0, algorithm, typeToString, stringToType, filterPredicate);

        ExternalSort.mergeSortedFiles(files, finalFFSForIndexing, comparator, StandardCharsets.UTF_8,
                true, true, algorithm, typeToString, stringToType);
        return finalFFSForIndexing;
    }

    private void compareFinalFFSAndMergedFFS(Path finalFfsPath, File mergedFile, MergeIncrementalFlatFileStore mergedStore) throws IOException {
        try (
                BufferedReader bufferedReaderFinalFilePath = IndexStoreUtils.createReader(finalFfsPath.toFile(), algorithm);
                BufferedReader bufferedReaderMergedFile = IndexStoreUtils.createReader(mergedFile, algorithm)
        ) {
            assertEquals(bufferedReaderFinalFilePath.lines().collect(Collectors.toList()),
                    bufferedReaderMergedFile.lines().collect(Collectors.toList()));
        }
        IndexStoreMetadata mergedIndexStoreMetadata = new IndexStoreMetadataOperatorImpl<IndexStoreMetadata>()
                .getIndexStoreMetadata(IndexStoreUtils.getMetadataFile(mergedFile, algorithm),
                        algorithm, new TypeReference<>() {
                        });

        IndexStoreMetadata indexStoreMetadataAtCheckpoint2 = new IndexStoreMetadataOperatorImpl<IndexStoreMetadata>()
                .getIndexStoreMetadata(IndexStoreUtils.getMetadataFile(finalFfsPath.toFile(), algorithm),
                        algorithm, new TypeReference<>() {
                        });

        assertEquals(mergedIndexStoreMetadata.getCheckpoint(), indexStoreMetadataAtCheckpoint2.getCheckpoint());
        assertEquals(mergedIndexStoreMetadata.getStoreType(), indexStoreMetadataAtCheckpoint2.getStoreType());
        assertEquals(mergedIndexStoreMetadata.getPreferredPaths(), indexStoreMetadataAtCheckpoint2.getPreferredPaths());
        assertEquals(mergedIndexStoreMetadata.getStrategy(), mergedStore.getStrategyName());
    }

    private File createIncrementalStore(String initialCheckpoint, ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore, Predicate<String> pathPredicate, String finalCheckpoint) throws IOException {
        IncrementalFlatFileStoreStrategy incrementalFlatFileStoreStrategy = createIncrementalStrategy(roStore,
                pathPredicate, initialCheckpoint, finalCheckpoint);
        File incrementalFile = incrementalFlatFileStoreStrategy.createSortedStoreFile();
        File incrementalStoreMetadata = incrementalFlatFileStoreStrategy.createMetadataFile();
        try (
                BufferedReader bufferedReaderIncrementalStoreMetadata = IndexStoreUtils.createReader(incrementalStoreMetadata, algorithm)
        ) {
            assertEquals(List.of(getMetadataJsonStringForIncrementalFFS(initialCheckpoint, finalCheckpoint, incrementalFlatFileStoreStrategy)),
                    bufferedReaderIncrementalStoreMetadata.lines().collect(Collectors.toList()));

        }
        return incrementalFile;
    }

    @NotNull
    private static String getMetadataJsonStringForIncrementalFFS(String initialCheckpoint, String finalCheckpoint, IncrementalFlatFileStoreStrategy incrementalFlatFileStoreStrategy) {
        return "{\"beforeCheckpoint\":\"" + initialCheckpoint + "\",\"afterCheckpoint\":\"" + finalCheckpoint + "\"," +
                "\"storeType\":\"" + incrementalFlatFileStoreStrategy.getStoreType() + "\"," +
                "\"strategy\":\"" + incrementalFlatFileStoreStrategy.getClass().getSimpleName() + "\"," +
                "\"preferredPaths\":[]}";
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
                algorithm,
                pathPredicate,
                Collections.emptyList(),
                checkpoint
        );
    }

    private IncrementalFlatFileStoreStrategy createIncrementalStrategy(ImmutablePair<MongoDocumentStore, DocumentNodeStore> roStore,
                                                                       Predicate<String> pathPredicate, String initialCheckpoint, String finalCheckpoint) {
        DocumentNodeStore readOnlyNodeStore = roStore.right;
        Set<String> preferredPathElements = Set.of();
        readOnlyNodeStore.retrieve(initialCheckpoint);
        readOnlyNodeStore.retrieve(finalCheckpoint);
        return new IncrementalFlatFileStoreStrategy(
                readOnlyNodeStore, initialCheckpoint, finalCheckpoint, sortFolder.getRoot(), preferredPathElements,
                algorithm, pathPredicate, new IncrementalFlatFileStoreNodeStateEntryWriter(fileBlobStore));
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

    private void createBaseContent(NodeStore rwNodeStore) throws CommitFailedException {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentBuilder = rootBuilder.child("content");
        contentBuilder.child("2022").child("02").setProperty("p1", "v202202");
        contentBuilder.child("2022").child("02").child("28").setProperty("p1", "v20220228");
        contentBuilder.child("2022").child("02").child("29").setProperty("p1", "v20220229");
        contentBuilder.child("2023").child("01").setProperty("p1", "v202301");

        @NotNull NodeBuilder contentDamBuilder = contentBuilder.child("dam");
        contentDamBuilder.child("1000").child("12").setProperty("p1", "v100012");
        contentDamBuilder.child("2022").child("02").setProperty("p1", "v202202");
        contentDamBuilder.child("2022").child("02").child("1").setProperty("p1", "v2022021");
        contentDamBuilder.child("2022").child("02").child("27").setProperty("p1", "v20220227");
        contentDamBuilder.child("2022").child("02").child("28").setProperty("p1", "v20220228");
        contentDamBuilder.child("2022").child("02").child("29").setProperty("p1", "v20220229");
        contentDamBuilder.child("2022").child("02").child("3").setProperty("p1", "v2022023");
        contentDamBuilder.child("2022").child("03").child("30").setProperty("p1", "v20220330");
        contentDamBuilder.child("2022").child("04").child("30").setProperty("p1", "v20220430");
        contentDamBuilder.child("2023").child("01").setProperty("p1", "v202301").child("01");
        contentDamBuilder.child("2023").child("02").setProperty("p1", "v202302");
        contentDamBuilder.child("2023").setProperty("p2", "v2023");
        contentDamBuilder.child("2023").child("02").child("28").setProperty("p1", "v20230228");
        contentDamBuilder.child("2023").child("04").child("29").setProperty("p1", "v20230429");
        contentDamBuilder.child("2023").child("04").child("30").setProperty("p1", "v20230430");
        contentDamBuilder.child("2023").child("05").child("30").setProperty("p1", "v20230530");
        rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void createIncrementalContent(NodeStore rwNodeStore) throws CommitFailedException {
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("1000").child("12").setProperty("p2", "v100012"); // new property added
        contentDamBuilder.child("2022").child("02").setProperty("p1", "v202202-new");// property updated
        contentDamBuilder.child("2023").removeProperty("p2");// property deleted
        contentDamBuilder.child("2023").child("01").remove(); //deleted node
        contentDamBuilder.child("2023").child("03").setProperty("p1", "v202301"); //added node
        contentDamBuilder.child("2023").child("02").child("28").setProperty("p1", "v20230228");
        contentDamBuilder.child("2023").child("04").child("29").setProperty("p1", "v20230429");
        contentDamBuilder.child("2023").child("04").child("30").setProperty("p1", "v20230430");
        rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }


    @NotNull
    private static String[] getNodeStateAtCheckpoint1() {
        return new String[]{
                "/|{}",
                "/content|{}",
                "/content/2022|{}",
                "/content/2022/02|{\"p1\":\"v202202\"}",
                "/content/2022/02/28|{\"p1\":\"v20220228\"}",
                "/content/2022/02/29|{\"p1\":\"v20220229\"}",
                "/content/2023|{}",
                "/content/2023/01|{\"p1\":\"v202301\"}",
                "/content/dam|{}",
                "/content/dam/1000|{}",
                "/content/dam/1000/12|{\"p1\":\"v100012\"}",
                "/content/dam/2022|{}",
                "/content/dam/2022/02|{\"p1\":\"v202202\"}",
                "/content/dam/2022/02/1|{\"p1\":\"v2022021\"}",
                "/content/dam/2022/02/27|{\"p1\":\"v20220227\"}",
                "/content/dam/2022/02/28|{\"p1\":\"v20220228\"}",
                "/content/dam/2022/02/29|{\"p1\":\"v20220229\"}",
                "/content/dam/2022/02/3|{\"p1\":\"v2022023\"}",
                "/content/dam/2022/03|{}",
                "/content/dam/2022/03/30|{\"p1\":\"v20220330\"}",
                "/content/dam/2022/04|{}",
                "/content/dam/2022/04/30|{\"p1\":\"v20220430\"}",
                "/content/dam/2023|{\"p2\":\"v2023\"}",
                "/content/dam/2023/01|{\"p1\":\"v202301\"}",
                "/content/dam/2023/01/01|{}",
                "/content/dam/2023/02|{\"p1\":\"v202302\"}",
                "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}",
                "/content/dam/2023/04|{}",
                "/content/dam/2023/04/29|{\"p1\":\"v20230429\"}",
                "/content/dam/2023/04/30|{\"p1\":\"v20230430\"}",
                "/content/dam/2023/05|{}",
                "/content/dam/2023/05/30|{\"p1\":\"v20230530\"}"

        };
    }

    @NotNull
    private static String[] getNodeStateAtCheckpoint2() {
        return new String[]{"/|{}",
                "/content|{}",
                "/content/2022|{}",
                "/content/2022/02|{\"p1\":\"v202202\"}",
                "/content/2022/02/28|{\"p1\":\"v20220228\"}",
                "/content/2022/02/29|{\"p1\":\"v20220229\"}",
                "/content/2023|{}",
                "/content/2023/01|{\"p1\":\"v202301\"}",
                "/content/dam|{}",
                "/content/dam/1000|{}",
                "/content/dam/1000/12|{\"p1\":\"v100012\",\"p2\":\"v100012\"}",
                "/content/dam/2022|{}",
                "/content/dam/2022/02|{\"p1\":\"v202202-new\"}",
                "/content/dam/2022/02/1|{\"p1\":\"v2022021\"}",
                "/content/dam/2022/02/27|{\"p1\":\"v20220227\"}",
                "/content/dam/2022/02/28|{\"p1\":\"v20220228\"}",
                "/content/dam/2022/02/29|{\"p1\":\"v20220229\"}",
                "/content/dam/2022/02/3|{\"p1\":\"v2022023\"}",
                "/content/dam/2022/03|{}",
                "/content/dam/2022/03/30|{\"p1\":\"v20220330\"}",
                "/content/dam/2022/04|{}",
                "/content/dam/2022/04/30|{\"p1\":\"v20220430\"}",
                "/content/dam/2023|{}",
                "/content/dam/2023/02|{\"p1\":\"v202302\"}",
                "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}",
                "/content/dam/2023/03|{\"p1\":\"v202301\"}",
                "/content/dam/2023/04|{}",
                "/content/dam/2023/04/29|{\"p1\":\"v20230429\"}",
                "/content/dam/2023/04/30|{\"p1\":\"v20230430\"}",
                "/content/dam/2023/05|{}",
                "/content/dam/2023/05/30|{\"p1\":\"v20230530\"}",
        };
    }

    private static String[] getNodeStatesWithPrefferedPathsAndPathPredicatesOverBaseFFSAfterMerging() {
        return new String[]{
                "/content/dam|{}",
                "/content/dam/1000|{}",
                "/content/dam/1000/12|{\"p1\":\"v100012\",\"p2\":\"v100012\"}",
                "/content/dam/2022|{}",
                "/content/dam/2022/04|{}",
                "/content/dam/2022/04/30|{\"p1\":\"v20220430\"}",
                "/content/dam/2022/02|{\"p1\":\"v202202-new\"}",
                "/content/dam/2022/02/29|{\"p1\":\"v20220229\"}",
                "/content/dam/2022/02/1|{\"p1\":\"v2022021\"}",
                "/content/dam/2022/02/27|{\"p1\":\"v20220227\"}",
                "/content/dam/2022/02/28|{\"p1\":\"v20220228\"}",
                "/content/dam/2022/02/3|{\"p1\":\"v2022023\"}",
                "/content/dam/2022/03|{}",
                "/content/dam/2022/03/30|{\"p1\":\"v20220330\"}",
                "/content/dam/2023|{}",
                "/content/dam/2023/04|{}",
                "/content/dam/2023/04/29|{\"p1\":\"v20230429\"}",
                "/content/dam/2023/04/30|{\"p1\":\"v20230430\"}",
                "/content/dam/2023/02|{\"p1\":\"v202302\"}",
                "/content/dam/2023/02/28|{\"p1\":\"v20230228\"}",
                "/content/dam/2023/03|{\"p1\":\"v202301\"}",
                "/content/dam/2023/05|{}",
                "/content/dam/2023/05/30|{\"p1\":\"v20230530\"}",
        };
    }

}
