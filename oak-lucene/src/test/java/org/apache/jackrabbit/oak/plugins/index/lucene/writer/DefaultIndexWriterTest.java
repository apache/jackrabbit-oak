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

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultIndexWriterTest {
    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void lazyInit() throws Exception {
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        DefaultIndexWriter writer = new DefaultIndexWriter(defn, builder, null, INDEX_DATA_CHILD_NAME, SUGGEST_DATA_CHILD_NAME, false);
        assertFalse(writer.close(0));
    }

    @Test
    public void writeInitializedUponReindex() throws Exception {
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        DefaultIndexWriter writer = new DefaultIndexWriter(defn, builder, null, INDEX_DATA_CHILD_NAME, SUGGEST_DATA_CHILD_NAME, true);
        assertTrue(writer.close(0));
    }

    @Test
    public void indexUpdated() throws Exception {
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        DefaultIndexWriter writer = new DefaultIndexWriter(defn, builder, null, INDEX_DATA_CHILD_NAME, SUGGEST_DATA_CHILD_NAME, false);

        Document document = new Document();
        document.add(newPathField("/a/b"));

        writer.updateDocument("/a/b", document);

        assertTrue(writer.close(0));
    }
}