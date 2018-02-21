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

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.binary.BinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class LuceneIndexerTest {
    private NodeState root = INITIAL_CONTENT;

    @Test
    public void nodeIndexed_WithIncludedPaths() throws Exception{
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        idxb.includedPaths("/content");

        NodeState defn = idxb.build();
        IndexDefinition idxDefn = new IndexDefinition(root, defn, "/oak:index/testIndex");

        NodeBuilder builder = root.builder();
        LuceneIndexer indexer = new LuceneIndexer(idxDefn, mock(LuceneIndexWriter.class), builder,
                mock(BinaryTextExtractor.class), mock(IndexingProgressReporter.class));

        NodeState testNode = EMPTY_NODE.builder().setProperty("foo","bar").getNodeState();

        assertTrue(indexer.index(new NodeStateEntry(testNode, "/content/x")));
        assertFalse(indexer.index(new NodeStateEntry(testNode, "/x")));
        assertFalse(indexer.index(new NodeStateEntry(testNode, "/")));
    }

}