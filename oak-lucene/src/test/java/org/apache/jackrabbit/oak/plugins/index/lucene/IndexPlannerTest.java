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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import static org.junit.Assert.assertNotNull;

public class IndexPlannerTest {
    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void planForSortField() throws Exception{
        NodeBuilder defn = LuceneIndexHelper.newLucenePropertyIndexDefinition(builder, "test", ImmutableSet.of("foo"), "async");
        IndexNode node = createIndexNode(new IndexDefinition(defn));
        IndexPlanner planner = new IndexPlanner(node, "/foo", createFilter("nt:base"),
                ImmutableList.of(new OrderEntry("foo", Type.LONG, OrderEntry.Order.ASCENDING)));
        assertNotNull(planner.getPlan());
    }

    private IndexNode createIndexNode(IndexDefinition defn) throws IOException {
        return new IndexNode("foo", defn, createSampleDirectory());
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private static Directory createSampleDirectory() throws IOException {
        Directory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(VERSION, LuceneIndexConstants.ANALYZER);
        IndexWriter writer = new  IndexWriter(dir, config);
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Field.Store.NO));
        writer.addDocument(doc);
        writer.close();
        return dir;
    }
}
