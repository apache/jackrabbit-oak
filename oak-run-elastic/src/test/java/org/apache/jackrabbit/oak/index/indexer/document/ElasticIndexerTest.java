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
package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;

public class ElasticIndexerTest {

    private NodeState root = INITIAL_CONTENT;

    @Test
    public void nodeIndexed_WithIncludedPaths() throws Exception {
        ElasticIndexDefinitionBuilder idxb = new ElasticIndexDefinitionBuilder();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        idxb.includedPaths("/content");

        NodeState defn = idxb.build();
        IndexDefinition idxDefn = new ElasticIndexDefinition(root, defn, "/oak:index/testIndex", "testPrefix");

        NodeBuilder builder = root.builder();

        FulltextIndexWriter indexWriter = new ElasticIndexWriterFactory(mock(ElasticConnection.class),
                mock(ElasticIndexTracker.class)).newInstance(idxDefn, defn.builder(), CommitInfo.EMPTY, false);
        ElasticIndexer indexer = new ElasticIndexer(idxDefn, mock(FulltextBinaryTextExtractor.class), builder,
                mock(IndexingProgressReporter.class), indexWriter, mock(ElasticIndexEditorProvider.class), mock(IndexHelper.class));

        NodeState testNode = EMPTY_NODE.builder().setProperty("foo", "bar").getNodeState();

        assertTrue(indexer.index(new NodeStateEntry.NodeStateEntryBuilder(testNode, "/content/x").build()));
        assertFalse(indexer.index(new NodeStateEntry.NodeStateEntryBuilder(testNode, "/x").build()));
        assertFalse(indexer.index(new NodeStateEntry.NodeStateEntryBuilder(testNode, "/").build()));
    }

}
