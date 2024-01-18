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

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("StaticPseudoFunctionalStyleMethod")
public class FlatFileStoreTest {

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    private final Set<String> preferred = singleton("jcr:content");
    private final Predicate<String> pathPredicate = path -> !path.equals("/remove");

    private void runBasicTest() throws Exception {
        List<String> paths = createTestPaths();
        FlatFileNodeStoreBuilder spyBuilder = Mockito.spy(new FlatFileNodeStoreBuilder(folder.getRoot()));
        FlatFileStore flatStore = spyBuilder.withBlobStore(new MemoryBlobStore())
                .withPreferredPathElements(preferred)
                .withPathPredicate(pathPredicate)
                .withNodeStateEntryTraverserFactory(range -> new NodeStateEntryTraverser("NS-1", null, null,null, range) {
                    @Override
                    public @NotNull Iterator<NodeStateEntry> iterator() {
                        return TestUtils.createEntries(paths).iterator();
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


    private List<String> createTestPaths() {
        return asList("/a", "/b", "/c", "/a/b w", "/a/jcr:content", "/a/b", "/", "/b/l", "/remove");
    }
}