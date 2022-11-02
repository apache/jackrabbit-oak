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
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_TRAVERSE_WITH_SORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlatFileNodeStoreBuilderTest {

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    private final NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory = range -> null;

    @Test
    public void defaultSortStrategy() throws Exception {
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withLastModifiedBreakPoints(Collections.emptyList())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
    }

    @Test
    public void sortStrategyBasedOnSystemProperty() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
            FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                    .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
            SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
            assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        }
    }

    @Test
    public void disableTraverseAndSortStrategyUsingSystemProperty() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "false");
            FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                    .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
            SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
            assertTrue(sortStrategy instanceof StoreAndSortStrategy);
        } finally {
            System.clearProperty(OAK_INDEXER_TRAVERSE_WITH_SORT);
        }
    }

    @Test
    public void sortStrategySystemPropertyPrecedence() throws Exception {
        try {
            System.setProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "false");
            System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
            FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                    .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
            SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
            assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
        } finally {
            System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
            System.clearProperty(OAK_INDEXER_TRAVERSE_WITH_SORT);
        }
    }

    @Test
    public void testBuildList() throws CompositeException, IOException {
        try {
            File flatFile = new File(getClass().getClassLoader().getResource("simple-split.json").getFile());
            System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, flatFile.getAbsolutePath());
            FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot()).withNodeStateEntryTraverserFactory(
                    nodeStateEntryTraverserFactory);
            IndexHelper indexHelper = mock(IndexHelper.class);
            IndexerSupport indexerSupport = mock(IndexerSupport.class);
            NodeState rootState = mock(NodeState.class);
            when(indexerSupport.retrieveNodeStateForCheckpoint()).thenReturn(rootState);
            List<FlatFileStore> storeList = builder.buildList(indexHelper, indexerSupport, null);
            assertEquals(1, storeList.size());
        } finally {
            System.clearProperty(OAK_INDEXER_SORTED_FILE_PATH);
        }
    }
}
