/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MongotIndexingTest {

    private static final String INDEX_PATH = "/oak:index/mongoSearch";

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void oakCommitWritesMongotSearchDocument() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore(initialContentWithIndex());
        try (MongoConnection connection = MongoConnection.create(
                mongo.getConnectionString(), mongo.getDatabaseName())) {
            MongotIndexEditorProvider editorProvider = new MongotIndexEditorProvider(connection, null);

            NodeBuilder content = store.getRoot().builder();
            content.child("content").child("a")
                    .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                    .setProperty("jcr:title", "Native Mongo connector")
                    .setProperty("priority", 10L);
            store.merge(content, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, editorProvider);
            try {
                async.run();
                assertFalse(async.isFailing());
            } finally {
                async.close();
            }

            NodeState definitionState = store.getRoot().getChildNode("oak:index")
                    .getChildNode("mongoSearch");
            MongotIndexDefinition definition = new MongotIndexDefinition(
                    store.getRoot(), definitionState, INDEX_PATH);
            MongoCollection<Document> collection = connection.getCollection(definition);
            Document indexed = collection.find(eq(MongoFieldNames.PATH, "/content/a")).first();

            assertEquals(1, collection.countDocuments(eq(MongoFieldNames.PATH, "/content/a")));
            assertEquals("Native Mongo connector", indexed.get(MongoFieldNames.TYPED, Document.class)
                    .get(MongoFieldNames.encodeProperty("jcr:title")));
            assertEquals(10L, indexed.get(MongoFieldNames.TYPED, Document.class)
                    .get(MongoFieldNames.encodeProperty("priority")));
        }
    }

    private static NodeState initialContentWithIndex() {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        new InitialContent().initialize(root);
        root.child("oak:index").getChildNode("counter").remove();

        IndexDefinitionBuilder definition = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        definition.evaluatePathRestrictions();
        definition.indexRule("nt:unstructured").property("jcr:title")
                .propertyIndex().analyzed().nodeScopeIndex()
                .property("priority").propertyIndex();
        root.child("oak:index").setChildNode("mongoSearch", definition.build());
        return root.getNodeState();
    }
}
