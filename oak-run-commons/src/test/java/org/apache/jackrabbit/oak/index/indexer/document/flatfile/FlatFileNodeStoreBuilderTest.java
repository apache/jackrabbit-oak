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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.OakInitializer;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.IndexerConfiguration;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORT_STRATEGY_TYPE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_TRAVERSE_WITH_SORT;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_USE_LZ4;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_USE_ZIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlatFileNodeStoreBuilderTest {

    private static final String BUILD_TARGET_FOLDER = "target";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    @Rule
    public final TestRule restoreSystemProperties = new RestoreSystemProperties();

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
        System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
    }

    @Test
    public void disableTraverseAndSortStrategyUsingSystemProperty() throws Exception {
        System.setProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "false");
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof StoreAndSortStrategy);
    }

    @Test
    public void sortStrategySystemPropertyPrecedence() throws Exception {
        System.setProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "false");
        System.setProperty(OAK_INDEXER_SORT_STRATEGY_TYPE, FlatFileNodeStoreBuilder.SortStrategyType.TRAVERSE_WITH_SORT.toString());
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot())
                .withNodeStateEntryTraverserFactory(nodeStateEntryTraverserFactory);
        SortStrategy sortStrategy = builder.createSortStrategy(builder.createStoreDir());
        assertTrue(sortStrategy instanceof TraverseWithSortStrategy);
    }

    @Test
    public void testBuild() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "false");
        File newFlatFile = getFile("simple-split.json", Compression.NONE);

        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
        assertBuild(newFlatFile.getParentFile().getAbsolutePath());
    }

    @Test
    public void testBuildGZIP() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "true");
        File newFlatFile = getFile("simple-split.json", Compression.GZIP);
        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
        
        assertBuild(newFlatFile.getParentFile().getAbsolutePath());
    }

    @Test
    public void testBuildLZ4() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "true");
        System.setProperty(OAK_INDEXER_USE_LZ4, "true");
        LZ4Compression compression = new LZ4Compression();

        File newFlatFile = getFile("simple-split.json", compression);
        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
        
        assertBuild(newFlatFile.getParentFile().getAbsolutePath());
    }

    @Test
    public void testBuildListNoSplit() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "false");

        File newFlatFile = getFile("complex-split.json", Compression.NONE);
        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
        
        assertBuildList(newFlatFile.getParentFile().getAbsolutePath(), false);
    }
    
    @Test
    public void testBuildListSplit() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "false");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_MIN_SPLIT_THRESHOLD, "0");

        File newFlatFile = getFile("complex-split.json", Compression.NONE);
        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
        
        assertBuildList(newFlatFile.getParentFile().getAbsolutePath(), true);
    }

    @Test
    public void testBuildListSplitGZIP() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "true");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_MIN_SPLIT_THRESHOLD, "0");
        
        File newFlatFile = getFile("complex-split.json", Compression.GZIP);
        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
                
        assertBuildList(newFlatFile.getParentFile().getAbsolutePath(), true);
    }

    @Test
    public void testBuildListSplitLZ4() throws CompositeException, IOException {
        System.setProperty(OAK_INDEXER_USE_ZIP, "true");
        System.setProperty(OAK_INDEXER_USE_LZ4, "true");
        System.setProperty(IndexerConfiguration.PROP_OAK_INDEXER_MIN_SPLIT_THRESHOLD, "0");
        LZ4Compression compression = new LZ4Compression();

        File newFlatFile = getFile("complex-split.json", compression);
        System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, newFlatFile.getParentFile().getAbsolutePath());
        
        assertBuildList(newFlatFile.getParentFile().getAbsolutePath(), true);
    }

    public void assertBuild(String dir) throws CompositeException, IOException {
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot()).withNodeStateEntryTraverserFactory(
            nodeStateEntryTraverserFactory);
        FlatFileStore store = builder.build();
        assertEquals(dir, store.getFlatFileStorePath());
    }
    
    private File getFile(String dataFile, Compression compression) throws IOException {
        File flatFile = new File(getClass().getClassLoader().getResource(dataFile).getFile());
        File newFlatFile = new File(folder.getRoot(), FlatFileStoreUtils.getSortedStoreFileName(compression));
        try (BufferedReader reader = FlatFileStoreUtils.createReader(flatFile, false);
            BufferedWriter writer = FlatFileStoreUtils.createWriter(newFlatFile, compression)) {
            IOUtils.copy(reader, writer);
        }
        return newFlatFile;
    }

    public void assertBuildList(String dir, boolean split) throws CompositeException, IOException {
        FlatFileNodeStoreBuilder builder = new FlatFileNodeStoreBuilder(folder.getRoot()).withNodeStateEntryTraverserFactory(
            nodeStateEntryTraverserFactory);
        IndexHelper indexHelper = mock(IndexHelper.class);
        when(indexHelper.getWorkDir()).thenReturn(new File(dir));
        IndexerSupport indexerSupport = mock(IndexerSupport.class);
        NodeState rootState = mock(NodeState.class);
        when(indexerSupport.retrieveNodeStateForCheckpoint()).thenReturn(rootState);
        
        List<FlatFileStore> storeList = builder.buildList(indexHelper, indexerSupport, mockIndexDefns());
        
        if (split) {
            assertEquals(new File(dir, "split").getAbsolutePath(), storeList.get(0).getFlatFileStorePath());
            assertTrue(storeList.size() > 1);
        } else {
            assertEquals(dir, storeList.get(0).getFlatFileStorePath());
            assertEquals(1, storeList.size());
        }
    }

    private static Set<IndexDefinition> mockIndexDefns() {
        NodeStore store = new MemoryNodeStore();
        EditorHook hook = new EditorHook(
            new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider()));
        OakInitializer.initialize(store, new InitialContent(), hook);

        Set<IndexDefinition> defns = new HashSet<>();
        IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();
        defnBuilder.indexRule("dam:Asset");
        defnBuilder.aggregateRule("dam:Asset");
        IndexDefinition defn = IndexDefinition.newBuilder(store.getRoot(), defnBuilder.build(), "/foo").build();
        defns.add(defn);

        NodeTypeInfoProvider mockNodeTypeInfoProvider = Mockito.mock(NodeTypeInfoProvider.class);
        NodeTypeInfo mockNodeTypeInfo = Mockito.mock(NodeTypeInfo.class, "dam:Asset");
        Mockito.when(mockNodeTypeInfo.getNodeTypeName()).thenReturn("dam:Asset");
        Mockito.when(mockNodeTypeInfoProvider.getNodeTypeInfo("dam:Asset")).thenReturn(mockNodeTypeInfo);         
        
        return defns;
    }
}
