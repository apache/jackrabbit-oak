/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.util.MongotIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

public class MongotCommitSemanticsTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void realTimeCommitSynchronizesEveryAffectedIndex() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore(INITIAL_CONTENT);
        try (MongoConnection connection = MongoConnection.create(
                mongo.getConnectionString(), mongo.getDatabaseName())) {
            MongotIndexTracker tracker = new MongotIndexTracker(connection);
            ContentRepository repository = new Oak(store)
                    .with(new OpenSecurityProvider())
                    .with(new MongotIndexEditorProvider(connection, null))
                    .with(tracker)
                    .with(new MongotIndexProvider(tracker))
                    .createContentRepository();
            try (ContentSession session = repository.login(null, null)) {
                Root root = session.getLatestRoot();
                addIndex(root, "titleIndex", "title");
                addIndex(root, "summaryIndex", "summary");
                root.commit(Map.of("sync-mode", "rt"));

                Tree content = root.getTree("/").addChild("content");
                Tree article = content.addChild("article");
                article.setProperty("title", "immediate title");
                article.setProperty("summary", "immediate summary");
                root.commit(Map.of("sync-mode", "rt"));

                assertEquals(List.of("/content/article"),
                        directSearch(store, connection, "titleIndex", "title"));
                assertEquals(List.of("/content/article"),
                        directSearch(store, connection, "summaryIndex", "summary"));
            }
        }
    }

    private static void addIndex(Root root, String name, String property) {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.noAsync();
        builder.includedPaths("/content");
        builder.indexRule("nt:base").property(property)
                .propertyIndex().analyzed().nodeScopeIndex();
        builder.build(root.getTree("/oak:index").addChild(name));
    }

    private static List<String> directSearch(MemoryNodeStore store,
                                             MongoConnection connection,
                                             String indexName,
                                             String term) {
        String indexPath = "/oak:index/" + indexName;
        NodeState current = store.getRoot();
        MongotIndexDefinition definition = new MongotIndexDefinition(current,
                current.getChildNode("oak:index").getChildNode(indexName), indexPath);
        MongoCollection<Document> collection = connection.getCollection(definition);
        Document search = new Document("$search",
                new Document("index", definition.getSearchIndexName())
                        .append("text", new Document("path", MongoFieldNames.FULLTEXT)
                                .append("query", term)));
        return collection.aggregate(List.of(search)).into(new ArrayList<>()).stream()
                .map(result -> result.getString(MongoFieldNames.PATH)).toList();
    }
}
