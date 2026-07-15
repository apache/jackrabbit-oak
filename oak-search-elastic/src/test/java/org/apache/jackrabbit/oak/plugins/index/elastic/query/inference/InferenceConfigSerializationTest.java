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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the toString() methods of the inference-related classes which use JsopBuilder
 */
public class InferenceConfigSerializationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DEFAULT_CONFIG_PATH = InferenceConstants.DEFAULT_OAK_INDEX_INFERENCE_CONFIG_PATH;
    private static final String ENRICHER_CONFIG = "{\"enricher\":{\"config\":{\"vectorSpaces\":{\"semantic\":{\"pipeline\":{\"steps\":[{\"inputFields\":{\"description\":\"STRING\",\"title\":\"STRING\"},\"chunkingConfig\":{\"enabled\":true},\"name\":\"sentence-embeddings\",\"model\":\"text-embedding-ada-002\",\"optional\":true,\"type\":\"embeddings\"}]},\"default\":false}},\"version\":\"0.0.1\"}}}";
    private static final String DEFAULT_ENRICHER_STATUS_MAPPING = "{\"properties\":{\"processingTimeMs\":{\"type\":\"date\"},\"latestError\":{\"type\":\"keyword\",\"index\":false},\"errorCount\":{\"type\":\"short\"},\"status\":{\"type\":\"keyword\"}}}";
    private static final String DEFAULT_ENRICHER_STATUS_DATA = "{\"processingTimeMs\":0,\"latestError\":\"\",\"errorCount\":0,\"status\":\"PENDING\"}";

    private NodeBuilder rootBuilder;
    private NodeStore nodeStore;

    @Before
    public void setup() {
        // Initialize memory node store
        rootBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        nodeStore = new MemoryNodeStore(rootBuilder.getNodeState());
    }

    @After
    public void tearDown() {
        rootBuilder = null;
        nodeStore = null;
    }

    /**
     * Test for InferenceConfig.toString()
     */
    @Test
    public void testInferenceConfigToString() throws Exception {
        // Setup: Create a basic inference config
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add index config
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Initialize the inference config
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Get the toString representation
        String json = inferenceConfig.toString();

        // Verify it's valid JSON 
        JsonNode node = MAPPER.readTree(json);

        // Verify the structure
        assertTrue("JSON should contain 'type' key", node.has("type"));
        assertEquals("Type should be inferenceConfig", InferenceConfig.TYPE, node.get("type").asText());
        assertTrue("JSON should contain 'enabled' key", node.has("enabled"));
        assertTrue("enabled should be true", node.get("enabled").asBoolean());
        assertTrue("JSON should contain 'indexConfigs' key", node.has("indexConfigs"));
        assertTrue("indexConfigs should be an object", node.get("indexConfigs").isObject());
        assertTrue("indexConfigs should contain testIndex", node.get("indexConfigs").has(indexName));
    }

    /**
     * Test for InferenceIndexConfig.toString()
     */
    @Test
    public void testInferenceIndexConfigToString() throws Exception {
        // Create a simple index config
        NodeBuilder indexConfigBuilder = rootBuilder.child("testIndex");
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Create the index config object
        InferenceIndexConfig indexConfig = new InferenceIndexConfig("testIndex", indexConfigBuilder.getNodeState());

        // Get the toString representation
        String json = indexConfig.toString();

        // Verify it's valid JSON
        JsonNode node = MAPPER.readTree(json);

        // Verify the structure
        assertTrue("JSON should contain 'type' key", node.has("type"));
        assertEquals("Type should be inferenceIndexConfig", InferenceIndexConfig.TYPE, node.get("type").asText());
        assertTrue("JSON should contain 'enricherConfig' key", node.has(InferenceIndexConfig.ENRICHER_CONFIG));
        assertEquals("Enricher config should match", ENRICHER_CONFIG, node.get(InferenceIndexConfig.ENRICHER_CONFIG).asText());
        assertTrue("JSON should contain 'enabled' key", node.has(InferenceConstants.ENABLED));
        assertTrue("enabled should be true", node.get(InferenceConstants.ENABLED).asBoolean());
        assertTrue("JSON should contain 'inferenceModelConfigs' key", node.has("inferenceModelConfigs"));
        assertTrue("inferenceModelConfigs should be an object", node.get("inferenceModelConfigs").isObject());
    }

    /**
     * Test for InferenceModelConfig.toString()
     */
    @Test
    public void testInferenceModelConfigToString() throws Exception {
        // Create a model config with header and payload
        NodeBuilder modelConfigBuilder = rootBuilder.child("testModel");
        modelConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        modelConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        modelConfigBuilder.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        modelConfigBuilder.setProperty(InferenceModelConfig.MODEL, "test-model");
        modelConfigBuilder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://test-service");
        modelConfigBuilder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.85);
        modelConfigBuilder.setProperty(InferenceModelConfig.MIN_TERMS, 3);
        modelConfigBuilder.setProperty(InferenceModelConfig.TIMEOUT, 10000);
        modelConfigBuilder.setProperty(InferenceModelConfig.NUM_CANDIDATES, 50);
        modelConfigBuilder.setProperty(InferenceModelConfig.CACHE_SIZE, 200);

        // Create header node
        NodeBuilder headerBuilder = modelConfigBuilder.child(InferenceModelConfig.HEADER);
        headerBuilder.setProperty("Authorization", "Bearer test-token");
        headerBuilder.setProperty("Content-Type", "application/json");

        // Create payload node
        NodeBuilder payloadBuilder = modelConfigBuilder.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder.setProperty("model", "text-embedding-ada-002");
        payloadBuilder.setProperty("dimensions", 1536);

        // Create the model config object
        InferenceModelConfig modelConfig = new InferenceModelConfig("testModel", modelConfigBuilder.getNodeState());

        // Get the toString representation
        String json = modelConfig.toString();

        // Verify it's valid JSON
        JsonNode node = MAPPER.readTree(json);

        // Verify structure
        assertTrue("JSON should contain 'TYPE' key", node.has("type"));
        assertEquals("Type should match", InferenceModelConfig.TYPE, node.get("type").asText());
        assertTrue("JSON should contain 'model' key", node.has(InferenceModelConfig.MODEL));
        assertEquals("Model should match", "test-model", node.get(InferenceModelConfig.MODEL).asText());
        assertTrue("JSON should contain 'embeddingServiceUrl' key", node.has(InferenceModelConfig.EMBEDDING_SERVICE_URL));
        assertEquals("Service URL should match", "http://test-service", node.get(InferenceModelConfig.EMBEDDING_SERVICE_URL).asText());
        assertTrue("JSON should contain 'similarityThreshold' key", node.has(InferenceModelConfig.SIMILARITY_THRESHOLD));
        assertEquals("Similarity threshold should match", 0.85, node.get(InferenceModelConfig.SIMILARITY_THRESHOLD).asDouble(), 0.001);
        assertTrue("JSON should contain 'minTerms' key", node.has(InferenceModelConfig.MIN_TERMS));
        assertEquals("Min terms should match", 3, node.get(InferenceModelConfig.MIN_TERMS).asInt());
        assertTrue("JSON should contain 'isDefault' key", node.has(InferenceModelConfig.IS_DEFAULT));
        assertTrue("isDefault should be true", node.get(InferenceModelConfig.IS_DEFAULT).asBoolean());
        assertTrue("JSON should contain 'enabled' key", node.has(InferenceModelConfig.ENABLED));
        assertTrue("enabled should be true", node.get(InferenceModelConfig.ENABLED).asBoolean());
        assertTrue("JSON should contain 'header' key", node.has(InferenceModelConfig.HEADER));
        assertTrue("JSON should contain 'inferencePayload' key", node.has(InferenceModelConfig.INFERENCE_PAYLOAD));
        assertTrue("JSON should contain 'timeout' key", node.has(InferenceModelConfig.TIMEOUT));
        assertEquals("Timeout should match", 10000, node.get(InferenceModelConfig.TIMEOUT).asInt());
        assertTrue("JSON should contain 'numCandidates' key", node.has(InferenceModelConfig.NUM_CANDIDATES));
        assertEquals("Num candidates should match", 50, node.get(InferenceModelConfig.NUM_CANDIDATES).asInt());
        assertTrue("JSON should contain 'cacheSize' key", node.has(InferenceModelConfig.CACHE_SIZE));
        assertEquals("Cache size should match", 200, node.get(InferenceModelConfig.CACHE_SIZE).asInt());
    }

    /**
     * Test for InferenceHeaderPayload.toString()
     */
    @Test
    public void testInferenceHeaderPayloadToString() throws Exception {
        // Create a header payload
        NodeBuilder headerBuilder = rootBuilder.child("header");
        headerBuilder.setProperty("Authorization", "Bearer test-token");
        headerBuilder.setProperty("Content-Type", "application/json");

        // Create the header payload object
        InferenceHeaderPayload headerPayload = new InferenceHeaderPayload(headerBuilder.getNodeState());

        // Get the toString representation
        String json = headerPayload.toString();

        // Verify it's valid JSON
        JsonNode node = MAPPER.readTree(json);

        // Verify structure
        assertTrue("JSON should contain Authorization", node.has("Authorization"));
        assertEquals("Authorization should match", "Bearer test-token", node.get("Authorization").asText());
        assertTrue("JSON should contain Content-Type", node.has("Content-Type"));
        assertEquals("Content-Type should match", "application/json", node.get("Content-Type").asText());
    }

    /**
     * Test for InferencePayload.toString()
     */
    @Test
    public void testInferencePayloadToString() throws Exception {
        // Create a payload
        NodeBuilder payloadBuilder = rootBuilder.child("payload");
        payloadBuilder.setProperty("model", "text-embedding-ada-002");
        payloadBuilder.setProperty("dimensions", 1536);

        // Create the payload object
        InferencePayload payload = new InferencePayload("testModel", payloadBuilder.getNodeState());

        // Get the toString representation
        String json = payload.toString();

        // Verify it's valid JSON
        JsonNode node = MAPPER.readTree(json);

        // Verify structure
        assertTrue("JSON should contain model", node.has("model"));
        assertEquals("Model should match", "text-embedding-ada-002", node.get("model").asText());
        assertTrue("JSON should contain dimensions", node.has("dimensions"));
        assertEquals("Dimensions should match", 1536, node.get("dimensions").asInt());
    }

    /**
     * Test for EnricherStatus.toString()
     */
    @Test
    public void testEnricherStatusToString() throws Exception {
        // Setup: Create a node structure with enricher status data
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        NodeBuilder enrichNode = inferenceConfigBuilder.child(InferenceConstants.ENRICH_NODE);
        enrichNode.setProperty(InferenceConstants.ENRICHER_STATUS_MAPPING, DEFAULT_ENRICHER_STATUS_MAPPING);
        enrichNode.setProperty(InferenceConstants.ENRICHER_STATUS_DATA, DEFAULT_ENRICHER_STATUS_DATA);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create the enricher status object
        EnricherStatus status = new EnricherStatus(nodeStore, DEFAULT_CONFIG_PATH);

        // Get the toString representation
        String json = status.toString();

        // Verify it's valid JSON
        JsonNode node = MAPPER.readTree(json);

        // Verify structure
        assertTrue("JSON should contain enricherStatusMapping", node.has(InferenceConstants.ENRICHER_STATUS_MAPPING));
        JsonNode mappingNode = MAPPER.readTree(node.get(InferenceConstants.ENRICHER_STATUS_MAPPING).asText());
        assertTrue("Mapping should contain properties", mappingNode.has("properties"));

        assertTrue("JSON should contain enricherStatusData", node.has("enricherStatusData"));
        JsonNode statusData = node.get("enricherStatusData");
        assertTrue("Status data should contain processingTimeMs", statusData.has("processingTimeMs"));
        assertEquals("Processing time should be 0", 0, statusData.get("processingTimeMs").asInt());
        assertTrue("Status data should contain status", statusData.has("status"));
        assertEquals("Status should be PENDING", "PENDING", statusData.get("status").asText());
        assertTrue("Status data should contain errorCount", statusData.has("errorCount"));
        assertEquals("Error count should be 0", 0, statusData.get("errorCount").asInt());
        assertTrue("Status data should contain latestError", statusData.has("latestError"));
        assertEquals("Latest error should be empty", "", statusData.get("latestError").asText());
    }

    /**
     * More comprehensive test for InferenceConfig.toString() to verify all fields
     */
    @Test
    public void testComprehensiveInferenceConfigToString() throws Exception {
        // Setup: Create a basic inference config
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add index config
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Add enricher status
        NodeBuilder enrichNode = inferenceConfigBuilder.child(InferenceConstants.ENRICH_NODE);
        enrichNode.setProperty(InferenceConstants.ENRICHER_STATUS_MAPPING, DEFAULT_ENRICHER_STATUS_MAPPING);
        enrichNode.setProperty(InferenceConstants.ENRICHER_STATUS_DATA, DEFAULT_ENRICHER_STATUS_DATA);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Initialize the inference config
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Get the toString representation
        String json = inferenceConfig.toString();

        // Verify it's valid JSON
        JsonNode node = MAPPER.readTree(json);

        // Verify the structure includes all fields from the toString method
        assertTrue("JSON should contain 'type' key", node.has("type"));
        assertEquals("Type should be inferenceConfig", InferenceConfig.TYPE, node.get("type").asText());

        assertTrue("JSON should contain 'enabled' key", node.has("enabled"));
        assertTrue("enabled should be true", node.get("enabled").asBoolean());

        assertTrue("JSON should contain 'inferenceConfigPath' key", node.has("inferenceConfigPath"));
        assertEquals("inferenceConfigPath should match", DEFAULT_CONFIG_PATH, node.get("inferenceConfigPath").asText());

        assertTrue("JSON should contain 'currentInferenceConfig' key", node.has("currentInferenceConfig"));
        assertTrue("currentInferenceConfig should not be empty", !node.get("currentInferenceConfig").asText().isEmpty());

        assertTrue("JSON should contain 'activeInferenceConfig' key", node.has("activeInferenceConfig"));
        assertTrue("activeInferenceConfig should not be empty", !node.get("activeInferenceConfig").asText().isEmpty());

        assertTrue("JSON should contain 'isInferenceEnabled' key", node.has("isInferenceEnabled"));
        assertTrue("isInferenceEnabled should be true", node.get("isInferenceEnabled").asBoolean());

        assertTrue("JSON should contain 'indexConfigs' key", node.has("indexConfigs"));
        assertTrue("indexConfigs should be an object", node.get("indexConfigs").isObject());
        assertTrue("indexConfigs should contain testIndex", node.get("indexConfigs").has(indexName));

        assertTrue("JSON should contain ':enrich' key", node.has(":enrich"));
        JsonNode enrichNode2 = node.get(":enrich");
        assertTrue("enrichNode should contain 'enricherStatusMapping'", enrichNode2.has(InferenceConstants.ENRICHER_STATUS_MAPPING));
        assertTrue("enrichNode should contain 'enricherStatusData'", enrichNode2.has(InferenceConstants.ENRICHER_STATUS_DATA));
    }

    /**
     * Helper method to create node paths
     */
    private NodeBuilder createNodePath(NodeBuilder rootBuilder, String path) {
        NodeBuilder currentBuilder = rootBuilder;
        for (String element : path.split("/")) {
            if (!element.isEmpty()) {
                currentBuilder = currentBuilder.child(element);
            }
        }
        return currentBuilder;
    }
} 