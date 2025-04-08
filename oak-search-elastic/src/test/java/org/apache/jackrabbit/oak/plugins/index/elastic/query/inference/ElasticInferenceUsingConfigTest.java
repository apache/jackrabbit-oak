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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpSerializer;
import co.elastic.clients.json.JsonpUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConstants.ENRICHER_CONFIG;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConstants.INFERENCE_CONFIG_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConstants.INFERENCE_INDEX_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticInferenceUsingConfigTest extends ElasticAbstractQueryTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(WireMockConfiguration.options().dynamicPort());

    private String enricherConfig = "{\"cais\":{\"config\":{\"vectorSpaces\":{\"semantic\":{\"pipeline\":{\"steps\":[{\"inputFields\":{\"description\":\"STRING\",\"title\":\"STRING\"},\"chunkingConfig\":{\"enabled\":true},\"name\":\"sentence-embeddings\",\"model\":\"text-embedding-ada-002\",\"optional\":true,\"type\":\"embeddings\"}]},\"default\":false}},\"version\":\"0.0.1\"}}}";
//    private String enricherConfig = "{\"a\":\"b\",\"c\":{\"d\":\"e\"}}";
    @Test
    public void inferenceConfigStoredInIndexMetadata() throws CommitFailedException, JsonProcessingException {
        String indexName = UUID.randomUUID().toString();
        // check that the inference config
        @NotNull Tree tree = root.getTree("/oak:index");
        // Create inference config structure
        Tree inferenceConfig =  tree.addChild("inferenceConfig");
        inferenceConfig.setProperty(InferenceConstants.ENABLED, true);
        inferenceConfig.setProperty(INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_INDEX_CONFIG);

        // Add inferenceIndexConfig
        Tree inferenceIndexConfig = inferenceConfig.addChild(indexName);
        inferenceIndexConfig.setProperty(INFERENCE_CONFIG_TYPE, INFERENCE_INDEX_CONFIG);
        inferenceIndexConfig.setProperty(ENRICHER_CONFIG, enricherConfig);
        // Add inference model1 configuration
        Tree inferenceModelConfig1 = inferenceIndexConfig.addChild("inferenceModel1");
        inferenceModelConfig1.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        inferenceModelConfig1.setProperty(INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        inferenceModelConfig1.setProperty(InferenceModelConfig.MODEL, "test-model1");
        inferenceModelConfig1.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080");
        inferenceModelConfig1.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        inferenceModelConfig1.setProperty(InferenceModelConfig.MIN_TERMS, 3L);
        inferenceModelConfig1.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        inferenceModelConfig1.setProperty(InferenceModelConfig.ENABLED, true);

        // Setup header configuration
        Tree header1 = inferenceModelConfig1.addChild(InferenceModelConfig.HEADER);
        header1.setProperty("headerKey1_1", "headerValue1_1");
        header1.setProperty("headerKey2_1", "headerValue2_1");

        // Setup payload configuration
        Tree payload1 = inferenceModelConfig1.addChild(InferenceModelConfig.INFERENCE_PAYLOAD);
        payload1.setProperty("textKey", "text1");
        payload1.setProperty("dimension", 1536);
        payload1.setProperty("model", "model-name-of-inference-model1");

        // Add inference model2 configuration
        Tree inferenceModelConfig2 = inferenceIndexConfig.addChild("inferenceModel2");
        inferenceModelConfig2.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        inferenceModelConfig2.setProperty(INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        inferenceModelConfig2.setProperty(InferenceModelConfig.MODEL, "test-model2");
        inferenceModelConfig2.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080");
        inferenceModelConfig2.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        inferenceModelConfig2.setProperty(InferenceModelConfig.MIN_TERMS, 3L);
        inferenceModelConfig2.setProperty(InferenceModelConfig.IS_DEFAULT, false);
        inferenceModelConfig2.setProperty(InferenceModelConfig.ENABLED, true);

        // Setup header configuration
        Tree header2 = inferenceModelConfig1.addChild(InferenceModelConfig.HEADER);
        header2.setProperty("headerKey1_2", "headerValue1_2");
        header2.setProperty("headerKey2_2", "headerValue2_2");

        // Setup payload configuration
        Tree payload2 = inferenceModelConfig2.addChild(InferenceModelConfig.INFERENCE_PAYLOAD);
        payload2.setProperty("textKey", "searchString2");
        payload2.setProperty("dimension", 1024);
        payload2.setProperty("model", "model-name-of-inference-model2");

        root.commit();

        IndexDefinitionBuilder builder = createIndex("a").noAsync();
//        builder.indexRule("nt:base").property("a").analyzed();
//
//        Tree inferenceConfig = builder.getBuilderTree().addChild(ElasticIndexDefinition.INFERENCE_CONFIG);
//        Tree inferenceProperties = inferenceConfig.addChild("properties");
//        Tree embeddings = inferenceProperties.addChild("embeddings");
//        embeddings.setProperty("fields", List.of("a"), Type.STRINGS);
//        Tree inferenceQueries = inferenceProperties.addChild("queries");
//        Tree semantic = inferenceQueries.addChild("semantic");
//        semantic.setProperty("serviceUrl", "http://localhost:" + wireMock.port());
//        semantic.setProperty("prefix", "!");
//        semantic.setProperty("minTerms", "2");
//
        Tree index = setIndex(indexName, builder);
        root.commit();

        ElasticIndexDefinition definition = getElasticIndexDefinition(index);

        IndexMappingRecord mapping = getMapping(index);
        Map<String, JsonData> meta = mapping.mappings().meta();
        assertNotNull(meta);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode1 = objectMapper.readTree(enricherConfig).get("cais");
        JsonNode jsonNode2 = objectMapper.readTree(meta.get("cais").toJson().toString());
        assertEquals(jsonNode1, jsonNode2);

        Map<String, Property> mappingProperties = mapping.mappings().properties();


    }

    @Test
    public void hybridSearch() throws Exception {
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/content")
                .indexRule("nt:base")
                .property("title").propertyIndex().analyzed().nodeScopeIndex()
                .property("description").propertyIndex().analyzed().nodeScopeIndex()
                .property("updatedBy").propertyIndex();

        Tree inferenceConfig = builder.getBuilderTree().addChild(ElasticIndexDefinition.INFERENCE_CONFIG);
        Tree embeddings = inferenceConfig.addChild("properties").addChild("embeddings");
        embeddings.setProperty("fields", List.of("title", "description"), Type.STRINGS);

        Tree queryConfig = inferenceConfig.addChild("queries").addChild("semantic");
        queryConfig.setProperty("serviceUrl", "http://localhost:" + wireMock.port() + "/get_embedding");
        queryConfig.setProperty("prefix", "?");
        queryConfig.setProperty("similarityThreshold", "0.75");
        queryConfig.setProperty("timeout", "1000");

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

        // this content is not enriched with embeddings on purpose
        Tree farm = content.addChild("farm");
        farm.setProperty("title", "Sustainable Farming Practices");
        farm.setProperty("description", "Sustainable farming practices are essential for preserving the environment. This article discusses crop rotation, soil health, and water conservation methods to reduce the carbon footprint of agriculture.");

        root.commit();

        // let the index catch up
        assertEventually(() -> assertEquals(7, countDocuments(index)));

        // this mimics the inference service by traversing the content and enriching it with embeddings
        ObjectMapper mapper = new JsonMapper();
        List<String> paths = executeQuery("select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and title is not null", SQL2);
        for (String path : paths) {
            URL json = this.getClass().getResource("/inference" + path + ".json");
            if (json != null) {
                @SuppressWarnings("unchecked")
                Map<String, Collection<Double>> map = mapper.readValue(json, Map.class);
                ObjectNode updateDoc = mapper.createObjectNode();
                ObjectNode inferenceNode = updateDoc.putObject(ElasticIndexDefinition.INFERENCE);
                ArrayNode embeddingsNode = inferenceNode.putObject("embeddings").putArray("value");
                inferenceNode.putObject("metadata").put("updatedAt", Instant.now().toEpochMilli());
                for (Double d : map.get("embedding")) {
                    embeddingsNode.add(d);
                }
                updateDocument(index, path, updateDoc);
            }
        }

        // let's instruct wiremock to return the embeddings for the queries as the inference service would
        try (Stream<Path> stream = Files.walk(Paths.get(this.getClass().getResource("/inference/queries").toURI()))) {
            stream.filter(Files::isRegularFile).forEach(queryFile -> {
                String query = FilenameUtils.removeExtension(queryFile.getFileName().toString()).replaceAll("_", " ");
                if (queryFile.toAbsolutePath().toString().contains("queries/faulty")) {
                    wireMock.stubFor(WireMock.post("/get_embedding")
                            .withRequestBody(WireMock.equalToJson("{\"text\":\"" + query + "\"}"))
                            .willReturn(WireMock.serverError()));
                } else if (queryFile.toAbsolutePath().toString().contains("delayed")) {
                    wireMock.stubFor(WireMock.post("/get_embedding")
                            .withRequestBody(WireMock.equalToJson("{\"text\":\"" + query + "\"}"))
                            .willReturn(WireMock.ok()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody("[]")
                                    .withFixedDelay(2000)));
                } else {
                    String json;
                    try {
                        json = IOUtils.toString(queryFile.toUri(), StandardCharsets.UTF_8);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    wireMock.stubFor(WireMock.post("/get_embedding")
                            .withRequestBody(WireMock.equalToJson("{\"text\":\"" + query + "\"}"))
                            .willReturn(WireMock.ok()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(json)));
                }
            });
        }

        Map<String, String> queryResults = Map.of(
                "a beginner guide to data manipulation in python", "/content/programming",
                "how to improve mental health through exercises", "/content/yoga",
                "nutritional advice for a healthier lifestyle", "/content/health",
                "technological advancements in electric vehicles", "/content/cars",
                "what are the key algorithms used in machine learning", "/content/ml"
        );

        assertEventually(() -> {

            for (Map.Entry<String, String> entry : queryResults.entrySet()) {
                String query = entry.getKey();
                String expectedPath = entry.getValue();
                String queryPath = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '?" + query + "')";
                List<String> results = executeQuery(queryPath, SQL2, true, true);
                assertEquals(expectedPath, results.get(0));

                // test that the same query does not return any result when the inference service is not invoked (no prefix)
                String queryPath2 = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '" + query + "')";
                assertQuery(queryPath2, List.of());
            }

            // test that a failure in the inference service does not prevent the query from returning results
            String queryPath3 = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '?machine learning')";
            assertQuery(queryPath3, List.of("/content/ml", "/content/programming"));

            // test that a delayed response from the inference service does not prevent the query from returning results
            String queryPath4 = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '?farming practices')";
            assertQuery(queryPath4, List.of("/content/farm"));
        });


        // let's check that inference data is not deleted when updating a document
        cars.setProperty("updatedBy", "John Doe");
        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and updatedBy = 'John Doe'", List.of("/content/cars")));

        ObjectNode carsDoc = getDocument(index, "/content/cars");
        assertNotNull(carsDoc.get(ElasticIndexDefinition.INFERENCE));
    }
}
