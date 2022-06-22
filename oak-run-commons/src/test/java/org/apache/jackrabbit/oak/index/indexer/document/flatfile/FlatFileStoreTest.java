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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.document.mongo.DocumentStoreSplitter;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_USE_ZIP;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.PROP_MERGE_TASK_BATCH_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.PROP_THREAD_POOL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("StaticPseudoFunctionalStyleMethod")
public class FlatFileStoreTest {

    static Logger logger = LoggerFactory.getLogger(FlatFileStoreTest.class);

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    private Set<String> preferred = singleton("jcr:content");
    private Predicate<String> pathPredicate = path -> !path.equals("/remove");

    private static final String EXCEPTION_MESSAGE = "Framed exception.";

    private void runBasicTest() throws Exception {
        List<String> paths = createTestPaths();
        FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot()));
        FlatFileStore flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withLastModifiedBreakPoints(Collections.singletonList(0L))
                .withPathPredicate(pathPredicate)
                .withNodeStateEntryTraverserFactory(new NodeStateEntryTraverserFactory() {
                    @Override
                    public NodeStateEntryTraverser create(MongoDocumentTraverser.TraversingRange range) {
                        return new NodeStateEntryTraverser("NS-1", null, null,
                                null, range) {
                            @Override
                            public @NotNull Iterator<NodeStateEntry> iterator() {
                                return TestUtils.createEntries(paths).iterator();
                            }
                        };
                    }
                })
                .build();

        List<String> entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                .map(NodeStateEntry::getPath)
                .collect(Collectors.toList());

        List<String> sortedPaths = TestUtils.sortPaths(paths, preferred);
        sortedPaths = TestUtils.extractPredicatePaths(sortedPaths, pathPredicate);

        assertEquals(sortedPaths, entryPaths);
    }

    @Test
    public void basicTestStoreAndSortStrategy() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.STORE_AND_SORT.toString());
            runBasicTest();
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        }
    }

    @Test
    public void basicTestTraverseAndSortStrategy() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
            runBasicTest();
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        }
    }

    @Test
    public void basicTestMultithreadedTraverseAndSortStrategy() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT.toString());
            runBasicTest();
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        }
    }

    @Test
    public void basicTestDefaultStrategy() throws Exception {
        runBasicTest();
    }

    @Test
    public void parallelDownload() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT.toString());
            List<TestMongoDoc> mongoDocs = getTestData();
            List<Long> lmValues = mongoDocs.stream().map(md -> md.lastModified).distinct().sorted().collect(Collectors.toList());
            List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lmValues.get(0), lmValues.get(lmValues.size() - 1), 10);
            FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot()));
            FlatFileStore flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                    .withPreferredPathElements(preferred)
                    .withLastModifiedBreakPoints(lastModifiedBreakpoints)
                    .withNodeStateEntryTraverserFactory(new TestNodeStateEntryTraverserFactory(mongoDocs))
                    .build();

            List<String> entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                    .map(NodeStateEntry::getPath)
                    .collect(Collectors.toList());

            List<String> sortedPaths = TestUtils.sortPaths(mongoDocs.stream().map(md -> md.path).collect(Collectors.toList()));

            assertEquals(sortedPaths, entryPaths);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        }
    }

    private FlatFileStore buildFlatFileStore(FlatFileNodeStoreBuilder spyBuilder, List<Long> lastModifiedBreakpoints,
                                    TestNodeStateEntryTraverserFactory nsetf, boolean expectException, Long dumpThreshold) throws Exception {
        boolean exceptionCaught = false;
        FlatFileStore flatFileStore = null;
        try {
            flatFileStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                    .withPreferredPathElements(preferred)
                    .withLastModifiedBreakPoints(lastModifiedBreakpoints)
                    .withNodeStateEntryTraverserFactory(nsetf)
                    .withDumpThreshold(dumpThreshold)
                    .build();
        } catch (CompositeException e) {
            exceptionCaught = true;
            e.logAllExceptions("Exceptions caught", logger);
            if (expectException) {
                assertEquals(EXCEPTION_MESSAGE, e.getSuppressed()[0].getCause().getMessage());
            }
        }
        assertEquals(expectException, exceptionCaught);
        return flatFileStore;
    }

    @Test
    public void resumePreviousUnfinishedDownload() throws Exception {
        Long dumpThreshold = 0L;
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT.toString());
            List<TestMongoDoc> mongoDocs = getTestData();
            List<Long> lmValues = mongoDocs.stream().map(md -> md.lastModified).distinct().sorted().collect(Collectors.toList());
            List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lmValues.get(0), lmValues.get(lmValues.size() - 1), 10);
            TestMemoryManager memoryManager = new TestMemoryManager(true);
            FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot(), memoryManager));
            TestNodeStateEntryTraverserFactory nsetf = new TestNodeStateEntryTraverserFactory(mongoDocs);
            nsetf.setDeliveryBreakPoint((int)(mongoDocs.size() * 0.25));
            FlatFileStore flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, true, dumpThreshold);
            assertNull(flatStore);
            spyBuilder.addExistingDataDumpDir(spyBuilder.getFlatFileStoreDir());
            nsetf.setDeliveryBreakPoint((int)(mongoDocs.size() * 0.50));
            flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, true, dumpThreshold);
            assertNull(flatStore);
            memoryManager.isMemoryLow = false;
            List<String> entryPaths;
            spyBuilder.addExistingDataDumpDir(spyBuilder.getFlatFileStoreDir());
            nsetf.setDeliveryBreakPoint(Integer.MAX_VALUE);
            flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, false, dumpThreshold);
            entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                    .map(NodeStateEntry::getPath)
                    .collect(Collectors.toList());

            List<String> sortedPaths = TestUtils.sortPaths(mongoDocs.stream().map(md -> md.path).collect(Collectors.toList()));
            assertEquals(mongoDocs.size(), nsetf.getTotalProvidedDocCount());
            assertEquals(sortedPaths, entryPaths);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        }
    }

    private boolean flatFileStoreMatchCondition(File dir, String filenamePattern, String entry) {
        Pattern pattern = Pattern.compile(filenamePattern, Pattern.CASE_INSENSITIVE);
        for (File innerDir : Objects.requireNonNull(dir.listFiles())) {
            if (innerDir.isDirectory()) {
                for (File innerFile : Objects.requireNonNull(innerDir.listFiles())) {
                    if (!pattern.matcher(innerFile.getName()).find()) {
                        continue;
                    }
                    List<String> lines = null;
                    try {
                        lines = FileUtils.readLines(innerFile, StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        fail("failed to read FlatFileStore");
                    }
                    for (String line : lines) {
                        if (line.contains(entry)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    // with larger size of dump threshold (which result in almost never dump),
    // fail in the middle, check that hidden node progress are saved
    @Test
    public void resumePreviousUnfinishedDownloadWithHiddenNode() throws Exception {
        Long dumpThreshold = FileUtils.ONE_MB;
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT.toString());
            System.setProperty(PROP_THREAD_POOL_SIZE, "1");
            System.setProperty(OAK_INDEXER_USE_ZIP, "false");
            List<TestMongoDoc> mongoDocs = new ArrayList<TestMongoDoc>() {{
                add(new TestMongoDoc("/10-0", 10));
                add(new TestMongoDoc("/10-0/:hidden1", 10));
                add(new TestMongoDoc("/10-0/:hidden2", 10));
                add(new TestMongoDoc("/10-0/:hidden3", 10));
                add(new TestMongoDoc("/10-0/:hidden4", 10));
                add(new TestMongoDoc("/10-1/end", 10));
            }};
            List<Long> lmValues = mongoDocs.stream().map(md -> md.lastModified).distinct().sorted().collect(Collectors.toList());
            List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lmValues.get(0), lmValues.get(lmValues.size() - 1), 1);
            TestMemoryManager memoryManager = new TestMemoryManager(true);
            FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot(), memoryManager));
            TestNodeStateEntryTraverserFactory nsetf = new TestNodeStateEntryTraverserFactory(mongoDocs);
            List<String> entryPaths;
            nsetf.setDeliveryBreakPoint(4);
            FlatFileStore flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, true, dumpThreshold);
            assertNull(flatStore);
            File existingFlatFileStoreDir1 = spyBuilder.getFlatFileStoreDir();
            assertTrue("flatFileStore should dump entry even if exception caught",
                    flatFileStoreMatchCondition(existingFlatFileStoreDir1, "flatfile", "/10-0"));
            assertTrue("flatFileStore should save hidden node progress on exception caught",
                    flatFileStoreMatchCondition(existingFlatFileStoreDir1, "last-saved","/10-0/:hidden3"));
            nsetf.setDeliveryBreakPoint(Integer.MAX_VALUE);
            spyBuilder.addExistingDataDumpDir(existingFlatFileStoreDir1);
            flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, false, dumpThreshold);
            entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                    .map(NodeStateEntry::getPath)
                    .collect(Collectors.toList());

            List<String> sortedPaths = TestUtils.sortPaths(mongoDocs.stream()
                    .map(md -> md.path)
                    .filter(path -> !NodeStateUtils.isHiddenPath(path))
                    .collect(Collectors.toList()));
            assertEquals(mongoDocs.size(), nsetf.getTotalProvidedDocCount());
            assertEquals(sortedPaths, entryPaths);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
            System.clearProperty(PROP_THREAD_POOL_SIZE);
            System.clearProperty(OAK_INDEXER_USE_ZIP);
        }
    }

    // with larger size of dump threshold (which result in almost never dump),
    // fail in the middle, check that there are data being dumped
    @Test
    public void resumePreviousUnfinishedDownloadWithGracefulDump() throws Exception {
        Long dumpThreshold = FileUtils.ONE_MB;
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT.toString());
            System.setProperty(PROP_THREAD_POOL_SIZE, "1");
            System.setProperty(OAK_INDEXER_USE_ZIP, "false");
            List<TestMongoDoc> mongoDocs = new ArrayList<TestMongoDoc>() {{
                add(new TestMongoDoc("/10-0", 10));
                add(new TestMongoDoc("/10-1", 10));
            }};
            List<Long> lmValues = mongoDocs.stream().map(md -> md.lastModified).distinct().sorted().collect(Collectors.toList());
            List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lmValues.get(0), lmValues.get(lmValues.size() - 1), 1);
            TestMemoryManager memoryManager = new TestMemoryManager(true);
            FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot(), memoryManager));
            TestNodeStateEntryTraverserFactory nsetf = new TestNodeStateEntryTraverserFactory(mongoDocs);
            List<String> entryPaths;
            nsetf.setDeliveryBreakPoint(1);
            FlatFileStore flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, true, dumpThreshold);
            assertNull(flatStore);
            File existingFlatFileStoreDir1 = spyBuilder.getFlatFileStoreDir();
            assertTrue("flatFileStore should dump entry even if exception caught",
                    flatFileStoreMatchCondition(existingFlatFileStoreDir1, "flatfile", "/10-0"));
            nsetf.setDeliveryBreakPoint(Integer.MAX_VALUE);
            spyBuilder.addExistingDataDumpDir(existingFlatFileStoreDir1);
            flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, false, dumpThreshold);
            entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                    .map(NodeStateEntry::getPath)
                    .collect(Collectors.toList());

            List<String> sortedPaths = TestUtils.sortPaths(mongoDocs.stream().map(md -> md.path).collect(Collectors.toList()));
            assertEquals(mongoDocs.size(), nsetf.getTotalProvidedDocCount());
            assertEquals(sortedPaths, entryPaths);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
            System.clearProperty(PROP_THREAD_POOL_SIZE);
            System.clearProperty(OAK_INDEXER_USE_ZIP);
        }
    }

    private void assertContainsMergeFolder(File dir, Boolean mustBeEmpty) {
        Boolean mergeFolderExist = false;
        for (File workDir : dir.listFiles()) {
            if (workDir.getName().equals("merge") && workDir.isDirectory()) {
                mergeFolderExist = true;
                if (mustBeEmpty) {
                    assertTrue("merge directory should not be empty", workDir.listFiles().length == 0);
                }
                break;
            }
        }
        assertTrue("merge directory should exist", mergeFolderExist);
    }

    @Test
    public void resumePreviousUnfinishedDownloadAndMerge() throws Exception {
        Long dumpThreshold = 0L;
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT.toString());
            System.setProperty(PROP_MERGE_TASK_BATCH_SIZE, "2");
            List<TestMongoDoc> mongoDocs = getTestData();
            List<Long> lmValues = mongoDocs.stream().map(md -> md.lastModified).distinct().sorted().collect(Collectors.toList());
            List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lmValues.get(0), lmValues.get(lmValues.size() - 1), 10);
            TestMemoryManager memoryManager = new TestMemoryManager(true);
            FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot(), memoryManager));
            TestNodeStateEntryTraverserFactory nsetf = new TestNodeStateEntryTraverserFactory(mongoDocs);
            nsetf.setDeliveryBreakPoint((int)(mongoDocs.size() * 0.50));
            FlatFileStore flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, true, dumpThreshold);
            assertNull(flatStore);
            File existingFlatFileStoreDir1 = spyBuilder.getFlatFileStoreDir();
            assertContainsMergeFolder(existingFlatFileStoreDir1, false);
            spyBuilder.addExistingDataDumpDir(existingFlatFileStoreDir1);
            nsetf.setDeliveryBreakPoint((int)(mongoDocs.size() * 0.75));
            flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, true, dumpThreshold);
            assertNull(flatStore);
            memoryManager.isMemoryLow = false;
            List<String> entryPaths;
            File existingFlatFileStoreDir2 = spyBuilder.getFlatFileStoreDir();
            assertContainsMergeFolder(existingFlatFileStoreDir2, false);
            spyBuilder.addExistingDataDumpDir(existingFlatFileStoreDir2);
            nsetf.setDeliveryBreakPoint(Integer.MAX_VALUE);
            flatStore = buildFlatFileStore(spyBuilder, lastModifiedBreakpoints, nsetf, false, dumpThreshold);
            entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                    .map(NodeStateEntry::getPath)
                    .collect(Collectors.toList());

            // Intermediate MergeFiles should be deleted after being merged
            assertContainsMergeFolder(existingFlatFileStoreDir1, true);
            assertContainsMergeFolder(existingFlatFileStoreDir2, true);
            assertContainsMergeFolder(spyBuilder.getFlatFileStoreDir(), true);

            List<String> sortedPaths = TestUtils.sortPaths(mongoDocs.stream().map(md -> md.path).collect(Collectors.toList()));
            // with multi-threaded download and multiple threads,
            // we can't expect that each entry is downloaded exactly once,
            // as there could be some overlap (the range check happends
            // _after_ retrieving the entry)
            assertTrue(mongoDocs.size() <= nsetf.getTotalProvidedDocCount());
            assertEquals(sortedPaths, entryPaths);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
            System.clearProperty(PROP_MERGE_TASK_BATCH_SIZE);
        }
    }

    private static class TestMemoryManager implements MemoryManager {

        boolean isMemoryLow;

        public TestMemoryManager(boolean isMemoryLow) {
            this.isMemoryLow = isMemoryLow;
        }

        @Override
        public Type getType() {
            return Type.SELF_MANAGED;
        }

        @Override
        public boolean isMemoryLow() {
            return isMemoryLow;
        }

        @Override
        public void changeMemoryUsedBy(long memory) {

        }

        @Override
        public boolean deregisterClient(String registrationID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> registerClient(MemoryManagerClient client) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestNodeStateEntryTraverserFactory implements NodeStateEntryTraverserFactory {

        final List<TestMongoDoc> mongoDocs;
        /**
         * The traversers will throw exception after these many documents have been returned in total from all traversers
         * created till now from this factory
         */
        final AtomicInteger breakAfterDelivering;
        /**
         * Keeps count of all the node states that have been iterated using all the {@link NodeStateEntryTraverser}s this
         * factory has created till now.
         */
        final AtomicInteger providedDocuments;

        public TestNodeStateEntryTraverserFactory(List<TestMongoDoc> mongoDocs) {
            this.mongoDocs = mongoDocs;
            this.breakAfterDelivering = new AtomicInteger(Integer.MAX_VALUE);
            this.providedDocuments = new AtomicInteger(0);
        }

        void setDeliveryBreakPoint(int value) {
            breakAfterDelivering.set(value);
        }

        @Override
        public NodeStateEntryTraverser create(MongoDocumentTraverser.TraversingRange range) {
            return new NodeStateEntryTraverser("NS-" + range.getLastModifiedRange().getLastModifiedFrom(),
                    null, null, null, range) {
                @Override
                public @NotNull Iterator<NodeStateEntry> iterator() {
                    List<TestMongoDoc> resultDocs = mongoDocs.stream().filter(doc -> range.getLastModifiedRange().contains(doc.lastModified) &&
                            (range.getStartAfterDocumentID() == null || range.getStartAfterDocumentID().compareTo(doc.id) < 0))
                            .sorted().collect(Collectors.toList()); // should be sorted in increasing order of (lastModificationTime, id)
                    if (resultDocs.isEmpty()) {
                        return Collections.emptyIterator();
                    }
                    Iterator<NodeStateEntry> nodeStateEntryIterator = createEntriesFromMongoDocs(resultDocs).iterator();
                    String traverserId = getId();
                    return new Iterator<NodeStateEntry>() {

                        // ensure we don't get a NullPointerException in logger.debug below
                        NodeStateEntry lastReturnedDoc = new NodeStateEntryBuilder(null, "/").build();

                        @Override
                        public boolean hasNext() {
                            return nodeStateEntryIterator.hasNext();
                        }

                        @Override
                        public NodeStateEntry next() {
                            // multiple threads could fail at the same time, that's fine
                            if (providedDocuments.get() >= breakAfterDelivering.get()) {
                                logger.debug("{} Breaking after getting docs with id {}", traverserId, lastReturnedDoc.getId());
                                throw new IllegalStateException(EXCEPTION_MESSAGE);
                            }
                            providedDocuments.incrementAndGet();
                            NodeStateEntry next = nodeStateEntryIterator.next();
                            lastReturnedDoc = next;
                            logger.debug("Returning {} to {} with LM={}", next.getPath(), traverserId, lastReturnedDoc.getLastModified());
                            return next;
                        }
                    };
                }
            };
        }

        int getTotalProvidedDocCount() {
            return providedDocuments.get();
        }
    }

    private List<String> createTestPaths() {
        return asList("/a", "/b", "/c", "/a/b w", "/a/jcr:content", "/a/b", "/", "/b/l", "/remove");
    }

    static Iterable<NodeStateEntry> createEntriesFromMongoDocs(List<TestMongoDoc> mongoDocs) {
        return Iterables.transform(mongoDocs, d -> new NodeStateEntry.NodeStateEntryBuilder(TestUtils.createNodeState(d.path),d.path)
                .withLastModified(d.lastModified).withID(d.id).build());
    }

    static class  TestMongoDoc implements Comparable<TestMongoDoc> {
        final String id;
        final String path;
        final long lastModified;

        public TestMongoDoc(String path, long lastModified) {
            this.path = path;
            this.lastModified = lastModified;
            int slashCount = 0, fromIndex = 0;
            while ( (fromIndex = path.indexOf("/", fromIndex) + 1) != 0) {
                slashCount++;
            }
            id = slashCount + ":" + path;
        }

        @Override
        public int compareTo(@NotNull FlatFileStoreTest.TestMongoDoc o) {
            int mod_comparison = Long.compare(lastModified, o.lastModified);
            if (mod_comparison != 0) {
                return mod_comparison;
            }
            return id.compareTo(o.id);
        }
    }

    private List<TestMongoDoc> getTestData() {
        return new ArrayList<TestMongoDoc>() {{
            add(new TestMongoDoc("/content", 10));
            add(new TestMongoDoc("/content/mysite", 20));
            add(new TestMongoDoc("/content/mysite/page1", 30));
            add(new TestMongoDoc("/content/mysite/page2", 30));
            add(new TestMongoDoc("/content/mysite/page3", 30));
            add(new TestMongoDoc("/content/mysite/page4", 30));
            add(new TestMongoDoc("/content/mysite/page5", 30));
            add(new TestMongoDoc("/content/mysite/page6", 30));
            add(new TestMongoDoc("/content/mysite/page1/child1", 40));
            add(new TestMongoDoc("/content/mysite/page2/child1", 40));
            add(new TestMongoDoc("/content/mysite/page3/child1", 40));
            add(new TestMongoDoc("/content/mysite/page4/child1", 40));
            add(new TestMongoDoc("/content/mysite/page5/child1", 40));
            add(new TestMongoDoc("/content/mysite/page6/child1", 40));
            add(new TestMongoDoc("/content/mysite/page1/child2", 80));
            add(new TestMongoDoc("/content/mysite/page2/child2", 80));
            add(new TestMongoDoc("/content/mysite/page3/child2", 80));
            add(new TestMongoDoc("/content/mysite/page4/child2", 80));
            add(new TestMongoDoc("/content/mysite/page5/child2", 80));
            add(new TestMongoDoc("/content/mysite/page6/child2", 80));
            add(new TestMongoDoc("/content/mysite/page1/child3", 120));
            add(new TestMongoDoc("/content/mysite/page2/child3", 120));
            add(new TestMongoDoc("/content/mysite/page3/child3", 120));
            add(new TestMongoDoc("/content/mysite/page4/child3", 120));
            add(new TestMongoDoc("/content/mysite/page5/child3", 120));
            add(new TestMongoDoc("/content/mysite/page6/child3", 120));
            add(new TestMongoDoc("/content/myassets", 20));
            add(new TestMongoDoc("/content/myassets/asset1", 30));
            add(new TestMongoDoc("/content/myassets/asset2", 30));
            add(new TestMongoDoc("/content/myassets/asset3", 30));
            add(new TestMongoDoc("/content/myassets/asset4", 30));
            add(new TestMongoDoc("/content/myassets/asset5", 30));
            add(new TestMongoDoc("/content/myassets/asset6", 30));
            add(new TestMongoDoc("/content/myassets/asset1/jcr:content", 50));
            add(new TestMongoDoc("/content/myassets/asset2/jcr:content", 50));
            add(new TestMongoDoc("/content/myassets/asset3/jcr:content", 50));
            add(new TestMongoDoc("/content/myassets/asset4/jcr:content", 50));
            add(new TestMongoDoc("/content/myassets/asset5/jcr:content", 50));
            add(new TestMongoDoc("/content/myassets/asset6/jcr:content", 50));
            add(new TestMongoDoc("/content/myassets/asset1/jcr:content/metadata", 100));
            add(new TestMongoDoc("/content/myassets/asset2/jcr:content/metadata", 100));
            add(new TestMongoDoc("/content/myassets/asset3/jcr:content/metadata", 100));
            add(new TestMongoDoc("/content/myassets/asset4/jcr:content/metadata", 100));
            add(new TestMongoDoc("/content/myassets/asset5/jcr:content/metadata", 100));
            add(new TestMongoDoc("/content/myassets/asset6/jcr:content/metadata", 100));

        }};
    }
}