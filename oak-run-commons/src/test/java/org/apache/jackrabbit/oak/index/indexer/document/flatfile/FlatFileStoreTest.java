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

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.document.mongo.DocumentStoreSplitter;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("StaticPseudoFunctionalStyleMethod")
public class FlatFileStoreTest {

    static Logger logger = LoggerFactory.getLogger(FlatFileStoreTest.class);

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    private Set<String> preferred = singleton("jcr:content");

    @Test
    public void basicTest() throws Exception {
        List<String> paths = createTestPaths();
        FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot()));
        Mockito.when(spyBuilder.getMemoryManager()).thenReturn(new DefaultMemoryManager(100*ONE_MB, 2*ONE_GB));
        FlatFileStore flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withLastModifiedBreakPoints(Collections.singletonList(0L))
                .withNodeStateEntryTraverserFactory(new NodeStateEntryTraverserFactory() {
                    @Override
                    public NodeStateEntryTraverser create(LastModifiedRange range) {
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

        assertEquals(sortedPaths, entryPaths);
    }

    @Test
    public void parallelDownload() throws Exception {
        Map<Long, List<String>> map = createPathsWithTimestamps();
        List<String> paths = map.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<Long> lastModifiedValues = new ArrayList<>(map.keySet());
        lastModifiedValues.sort(Long::compare);
        List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lastModifiedValues.get(0),
                lastModifiedValues.get(lastModifiedValues.size() - 1), 10);
        FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot()));
        Mockito.when(spyBuilder.getMemoryManager()).thenReturn(new DefaultMemoryManager(100*ONE_MB, 2*ONE_GB));
        FlatFileStore flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withLastModifiedBreakPoints(lastModifiedBreakpoints)
                .withNodeStateEntryTraverserFactory(new TestNodeStateEntryTraverserFactory(map, false))
                .build();

        List<String> entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                .map(NodeStateEntry::getPath)
                .collect(Collectors.toList());

        List<String> sortedPaths = TestUtils.sortPaths(paths);

        assertEquals(sortedPaths, entryPaths);
    }

    @Test
    public void resumePreviousUnfinishedDownload() throws Exception {
        Map<Long, List<String>> map = createPathsWithTimestamps();
        List<String> paths = map.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<Long> lastModifiedValues = new ArrayList<>(map.keySet());
        lastModifiedValues.sort(Long::compare);
        List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lastModifiedValues.get(0),
                lastModifiedValues.get(lastModifiedValues.size() - 1), 10);
        FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot()));
        TestMemoryManager memoryManager = new TestMemoryManager(true);
        Mockito.when(spyBuilder.getMemoryManager()).thenReturn(new TestMemoryManager(true));
        TestNodeStateEntryTraverserFactory nsetf = new TestNodeStateEntryTraverserFactory(map, true);
        FlatFileStore flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withLastModifiedBreakPoints(lastModifiedBreakpoints)
                .withNodeStateEntryTraverserFactory(nsetf)
                .build();

        List<String> entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                .map(NodeStateEntry::getPath)
                .collect(Collectors.toList());

        assertEquals(Collections.emptyList(), entryPaths);
        memoryManager.isMemoryLow = false;
        nsetf.interrupt = false;
        File oldDataDir = new File(BUILD_TARGET_FOLDER + "/old-data"+System.currentTimeMillis());
        FileUtils.copyDirectory(folder.getRoot().listFiles()[0], oldDataDir);
        flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withLastModifiedBreakPoints(lastModifiedBreakpoints)
                .withExistingDataDumpDir(oldDataDir)
                .withNodeStateEntryTraverserFactory(nsetf)
                .build();

        entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                .map(NodeStateEntry::getPath)
                .collect(Collectors.toList());

        List<String> sortedPaths = TestUtils.sortPaths(paths);
        assertEquals(paths.size(), nsetf.getTotalProvidedDocCount());
        assertEquals(sortedPaths, entryPaths);
        FileUtils.deleteDirectory(oldDataDir);
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
        public void deregisterClient(String registrationID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> registerClient(MemoryManagerClient client) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestNodeStateEntryTraverserFactory implements NodeStateEntryTraverserFactory {

        /**
         * Map of timestamps and paths which were created at those timestamps.
         */
        final Map<Long, List<String>> pathData;
        /**
         * If this is true, iterators obtained from {@link NodeStateEntryTraverser}s this factory creates, throw an
         * exception when reaching the middle of data they are iterating.
         */
        boolean interrupt;
        /**
         * Keeps count of all the node states that have been iterated using all the {@link NodeStateEntryTraverser}s this
         * factory has created till now.
         */
        final AtomicInteger providedDocuments;
        /**
         * Mapping from timestamps to the number of nodes states that have been iterated for those timestamps using the
         * {@link NodeStateEntryTraverser}s created by this factory.
         */
        final ConcurrentHashMap<Long, Integer> returnCounts;
        /**
         * This keeps count of the node states that will be returned again if the same instance of this factory is used
         * for creating {@link NodeStateEntryTraverser}s in a subsequent run of a failed flat file store creation.
         */
        final AtomicInteger duplicateDocs;

        public TestNodeStateEntryTraverserFactory(Map<Long, List<String>> pathData, boolean interrupt) {
            this.pathData = pathData;
            this.interrupt = interrupt;
            this.providedDocuments = new AtomicInteger(0);
            this.returnCounts = new ConcurrentHashMap<>();
            this.duplicateDocs = new AtomicInteger(0);
        }

        @Override
        public NodeStateEntryTraverser create(LastModifiedRange range) {
            return new NodeStateEntryTraverser("NS-" + range.getLastModifiedFrom(), null, null,
                    null, range) {
                @Override
                public @NotNull Iterator<NodeStateEntry> iterator() {
                    List<String> paths = new ArrayList<>();
                    pathData.keySet().stream().filter(range::contains).forEach(time -> paths.addAll(pathData.get(time)));
                    if (paths.isEmpty()) {
                        return Collections.emptyIterator();
                    }
                    Iterator<NodeStateEntry> nodeStateEntryIterator = TestUtils.createEntriesWithLastModified(paths,
                            range.getLastModifiedFrom()).iterator();
                    AtomicInteger returnCount = new AtomicInteger(0);
                    int breakPoint = paths.size()/2;
                    String traverserId = getId();
                    return new Iterator<NodeStateEntry>() {

                        long lastReturnedDocLastModified = -1;

                        @Override
                        public boolean hasNext() {
                            return nodeStateEntryIterator.hasNext();
                        }

                        @Override
                        public NodeStateEntry next() {
                            if (interrupt && returnCount.get() == breakPoint) {
                                duplicateDocs.addAndGet(returnCounts.getOrDefault(lastReturnedDocLastModified, 0));
                                throw new IllegalStateException("Framed exception.");
                            }
                            returnCount.incrementAndGet();
                            providedDocuments.incrementAndGet();
                            NodeStateEntry next = nodeStateEntryIterator.next();
                            logger.info("Returning {} to {}",next.getPath(), traverserId);
                            lastReturnedDocLastModified = next.getLastModified();
                            returnCounts.compute(next.getLastModified(), (k,v) -> v == null ? 1 : v+1);
                            return next;
                        }
                    };
                }
            };
        }

        int getTotalProvidedDocCount() {
            return providedDocuments.get() - duplicateDocs.get();
        }

    }

    private List<String> createTestPaths() {
        return asList("/a", "/b", "/c", "/a/b w", "/a/jcr:content", "/a/b", "/", "/b/l");
    }

    /**
     * @return a map with keys denoting timestamp and values denoting paths which were created at those timestamps.
     */
    private Map<Long, List<String>> createPathsWithTimestamps() {
        Map<Long, List<String>> map = new HashMap<>();
        map.put(10L, new ArrayList<String>() {{
            add("/a");
        }});
        map.put(20L, new ArrayList<String>() {{
            add("/b");
            add("/b/b");
        }});
        map.put(30L, new ArrayList<String>() {{
            add("/c");
            add("/c/c");
            add("/c/c/c");
        }});
        map.put(40L, new ArrayList<String>() {{
            add("/d");
            add("/d/d");
            add("/d/d/d");
            add("/d/d/d/d");
        }});
        map.put(50L, new ArrayList<String>() {{
            add("/e");
            add("/e/e");
            add("/e/e/e");
            add("/e/e/e/e");
            add("/e/e/e/e/e");
        }});
        return map;
    }

}