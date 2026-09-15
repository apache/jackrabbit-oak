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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotSearchEnvironmentTest {

    private static final String INDEX_NAME = "oak-task0-search";

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void indexesAndSearchesDocument() {
        MongoCollection<Document> documents = mongo.getDatabase().getCollection("documents");
        documents.insertMany(List.of(
                new Document("_id", "oak")
                        .append("title", "Oak connector proof")
                        .append("body", "A connector sends Oak content to Mongot."),
                new Document("_id", "other")
                        .append("title", "Elasticsearch baseline")
                        .append("body", "Elasticsearch remains the separate baseline.")
        ));

        documents.createSearchIndex(INDEX_NAME,
                new Document("mappings", new Document("dynamic", true)));
        mongo.awaitSearchIndexReady(documents, INDEX_NAME, Duration.ofMinutes(2));

        Document searchStage = new Document("$search", new Document("index", INDEX_NAME)
                .append("text", new Document("path", "body").append("query", "connector")));
        List<Document> results = documents.aggregate(List.of(searchStage)).into(new ArrayList<>());

        assertEquals(List.of("oak"), results.stream()
                .map(result -> result.getString("_id"))
                .toList());
    }
}
