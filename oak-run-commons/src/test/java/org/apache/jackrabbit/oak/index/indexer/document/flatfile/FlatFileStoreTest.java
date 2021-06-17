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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("StaticPseudoFunctionalStyleMethod")
public class FlatFileStoreTest {

    Logger logger = LoggerFactory.getLogger(FlatFileStoreTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Set<String> preferred = singleton("jcr:content");

    @Test
    public void basicTest() throws Exception {
        List<String> paths = createTestPaths();
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot());
        FlatFileStore flatStore = builder.withBlobStore(new MemoryBlobStore())
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
    public void parallelTest() throws Exception {
        Map<Long, List<String>> map = createPathsWithTimestamps();
        List<String> paths = map.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<Long> lastModifiedValues = new ArrayList<>(map.keySet());
        lastModifiedValues.sort(Long::compare);
        List<Long> lastModifiedBreakpoints = DocumentStoreSplitter.simpleSplit(lastModifiedValues.get(0),
                lastModifiedValues.get(lastModifiedValues.size() - 1), 10);
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot());
        FlatFileStore flatStore = builder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withLastModifiedBreakPoints(lastModifiedBreakpoints)
                .withNodeStateEntryTraverserFactory(new NodeStateEntryTraverserFactory() {
                    @Override
                    public NodeStateEntryTraverser create(LastModifiedRange range) {
                        return new NodeStateEntryTraverser("NS-" + range.getLastModifiedLowerBound(), null, null,
                                null, range) {
                            @Override
                            public @NotNull Iterator<NodeStateEntry> iterator() {
                                List<String> paths = new ArrayList<>();
                                map.keySet().stream().filter(range::contains).forEach(time -> paths.addAll(map.get(time)));
                                if (paths.isEmpty()) {
                                    return Collections.emptyIterator();
                                }
                                return TestUtils.createEntriesWithLastModified(paths, range.getLastModifiedLowerBound()).iterator();
                            }
                        };
                    }
                })
                .build();

        List<String> entryPaths = StreamSupport.stream(flatStore.spliterator(), false)
                .map(NodeStateEntry::getPath)
                .collect(Collectors.toList());

        List<String> sortedPaths = TestUtils.sortPaths(paths);

        assertEquals(sortedPaths, entryPaths);
    }

    private List<String> createTestPaths() {
        return asList("/a", "/b", "/c", "/a/b w", "/a/jcr:content", "/a/b", "/", "/b/l");
    }

    private Map<Long, List<String>> createPathsWithTimestamps() throws InterruptedException {
        Random random = new Random();
        Map<Long, List<String>> map = new HashMap<>();
        for (int i = 1; i <= 20; i++) {
            long cur = System.currentTimeMillis();
            List<String> paths = new ArrayList<>();
            for(int j = 1; j <= i*5; j++) {
                paths.add("/" + getRandomWord(random, 4 + random.nextInt(7)));
            }
            logger.debug("Putting " + cur + " - " + paths);
            map.put(cur, paths);
            Thread.sleep(2);
        }
        return map;
    }

    private String getRandomWord(Random random, int length) {
        String s = "";
        for (int i = 0; i < length; i++) {
            s += (char) ('a' + random.nextInt(26));
        }
        //adding nano time to reduce the possibility of an already generated word being generated again
        return s + System.nanoTime();
    }

}