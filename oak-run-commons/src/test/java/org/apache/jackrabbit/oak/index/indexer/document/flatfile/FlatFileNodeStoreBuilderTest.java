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

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_TRAVERSE_WITH_SORT;
import static org.junit.Assert.assertTrue;

public class FlatFileNodeStoreBuilderTest {

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    private final NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory = range -> null;

    @Test
    public void defaultSortStrategy() throws Exception {
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withLastModifiedBreakPoints(Collections.emptyList());
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof MultithreadedTraverseWithSortStrategy);
    }

    @Test
    public void sortStrategyBasedOnSystemProperty() throws Exception {
        System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
        System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
    }

    @Test
    public void disableTraverseAndSortStrategyUsingSystemProperty() throws Exception {
        System.setProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "false");
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof StoreAndSortStrategy);
        System.clearProperty(OAK_INDEXER_TRAVERSE_WITH_SORT);
    }

    @Test
    public void sortStrategySystemPropertyPrecedence() throws Exception {
        System.setProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "false");
        System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
        System.clearProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
        System.clearProperty(OAK_INDEXER_TRAVERSE_WITH_SORT);
    }


}
