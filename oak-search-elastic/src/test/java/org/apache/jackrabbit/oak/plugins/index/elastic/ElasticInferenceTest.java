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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ElasticInferenceTest extends ElasticAbstractQueryTest {

    @Rule
    public WireMockRule wireMock= new WireMockRule(WireMockConfiguration.options().dynamicPort());

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(true);
    }

    @Test
    public void inferenceConfigStoredInIndexMetadata() throws CommitFailedException {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        Tree inferenceConfig = builder.getBuilderTree().addChild(ElasticIndexDefinition.INFERENCE_CONFIG);
        Tree embeddings = inferenceConfig.addChild("embeddings");
        embeddings.setProperty("serviceUrl", "http://localhost:" + wireMock.port());
        embeddings.setProperty("fields", List.of("a"), Type.STRINGS);
        builder.indexRule("nt:base").property("a").analyzed();
        String indexName = UUID.randomUUID().toString();
        Tree index = setIndex(indexName, builder);
        root.commit();

        ElasticIndexDefinition definition = getElasticIndexDefinition(index);

        IndexMappingRecord mapping = getMapping(index);
        Map<String, JsonData> meta = mapping.mappings().meta();
        JsonData inferenceConfigJson = meta.get(ElasticIndexDefinition.INFERENCE_CONFIG);
        assertNotNull(inferenceConfigJson);
        ElasticIndexDefinition.InferenceDefinition inferenceDefinition = inferenceConfigJson.to(ElasticIndexDefinition.InferenceDefinition.class);
        assertEquals(inferenceDefinition, definition.inferenceDefinition);
    }

    @Test
    public void semanticSearch() throws CommitFailedException {
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/content")
                .indexRule("nt:base")
                .property("title").propertyIndex().analyzed()
                .property("description").propertyIndex().analyzed();

        Tree inferenceConfig = builder.getBuilderTree().addChild(ElasticIndexDefinition.INFERENCE_CONFIG);
        Tree embeddings = inferenceConfig.addChild("embeddings");
        embeddings.setProperty("serviceUrl", "http://localhost:" + wireMock.port());
        embeddings.setProperty("fields", List.of("title", "description"), Type.STRINGS);

        String indexName = UUID.randomUUID().toString();
        Tree index = setIndex(indexName, builder);
        root.commit();

        // add content
        Tree content = root.getTree("/").addChild("content");
        Tree health = content.addChild("health");
        health.setProperty("title", "Healthy Eating for a Balanced Life");
        health.setProperty("description", "This article discusses how a well-balanced diet can lead to better health outcomes. It covers the importance of fruits, vegetables, lean proteins, and whole grains.");

        Tree cars = content.addChild("cars");
        cars.setProperty("title", "The Future of Electric Cars");
        cars.setProperty("description", "Electric vehicles are revolutionizing the automobile industry. This paper explores advancements in battery technology, charging infrastructure, and sustainability.");

        Tree programming = content.addChild("programming");
        programming.setProperty("title", "Mastering Python for Data Science");
        programming.setProperty("description", "A comprehensive guide to using Python for data science projects. Topics include data manipulation, visualization, and machine learning algorithms like decision trees and neural networks.");

        Tree ml = content.addChild("ml");
        ml.setProperty("title", "Introduction to Machine Learning");
        ml.setProperty("description", "This book introduces machine learning concepts, focusing on supervised and unsupervised learning techniques. It covers algorithms like linear regression, k-means clustering, and support vector machines.");

        Tree yoga = content.addChild("yoga");
        yoga.setProperty("title", "Yoga for Mental Wellness");
        yoga.setProperty("description", "The benefits of yoga for mental health are vast. This study shows how practicing yoga can reduce stress, anxiety, and improve overall well-being through breathing techniques and meditation.");

        root.commit();

        // let the index catch up
        assertEventually(() -> assertEquals(5, countDocuments(index)));

        ObjectMapper mapper = new JsonMapper();
        List<String> docs = executeQuery("select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and title is not null", SQL2);
        for (String doc : docs) {
//            Map<String, Object> docMap = mapper.readValue(doc, Map.class);
//            String path = (String) docMap.get("jcr:path");
//            System.out.println("Path: " + path);
        }

        wireMock.stubFor(WireMock.post("/get_embeddings")
                .withRequestBody(WireMock.equalToJson("{\"text\":\"pets playing in a park\"}"))
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"embeddings\":[0.1,0.2,0.3]}")));
    }

    @Test
    public void connectionCutOnQuery() throws Exception {
        String indexName = UUID.randomUUID().toString();
        setIndex(indexName, createIndex("propa", "propb").includedPaths("/test"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "a");
        test.addChild("b").setProperty("propa", "c");
        test.addChild("c").setProperty("propb", "e");
        root.commit(Map.of("sync-mode", "rt"));

        String query = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/test') and propa is not null";

        assertEventually(() -> {
            assertThat(explain(query), containsString("elasticsearch:" + indexName));
            assertQuery(query, List.of("/test/a", "/test/b"));
        });
    }
}
