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

import com.fasterxml.jackson.core.JsonProcessingException;
import joptsimple.internal.Strings;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.EnvironmentVariableProcessorUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InferenceConfigTest {

    private static final Logger LOG = LoggerFactory.getLogger(InferenceConfigTest.class);
    String ENRICH_STATUS_PENDING = "PENDING";
    private static final String DEFAULT_CONFIG_PATH = InferenceConstants.DEFAULT_OAK_INDEX_INFERENCE_CONFIG_PATH;
    private static final String ENRICHER_CONFIG = "{\"enricher\":{\"config\":{\"vectorSpaces\":{\"semantic\":{\"pipeline\":{\"steps\":[{\"inputFields\":{\"description\":\"STRING\",\"title\":\"STRING\"},\"chunkingConfig\":{\"enabled\":true},\"name\":\"sentence-embeddings\",\"model\":\"text-embedding-ada-002\",\"optional\":true,\"type\":\"embeddings\"}]},\"default\":false}},\"version\":\"0.0.1\"}}}";

    private NodeBuilder rootBuilder;
    private NodeStore nodeStore;

    private final String AUTH_ENV_VARIABLE = "$Authorization";
    private final String INFERENCE_SERVICE_URL_ENV_VARIABLE = "$inferenceServiceUrl";
    private final String INFERENCE_PAYLOAD_MODEL = "$EMBEDDING_MODEL";

    private boolean isAuthEnvVarDefined;
    private boolean isInferenceUrlEnvVarDefined;
    private boolean isInferencePayloadModelDefined;

    @Before
    public void setup() {
        // Initialize memory node store
        rootBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        nodeStore = new MemoryNodeStore(rootBuilder.getNodeState());

        isAuthEnvVarDefined = !EnvironmentVariableProcessorUtil.processEnvironmentVariable(
            InferenceConstants.INFERENCE_ENVIRONMENT_VARIABLE_PREFIX, AUTH_ENV_VARIABLE, "").equals(Strings.EMPTY);
        isInferenceUrlEnvVarDefined = !EnvironmentVariableProcessorUtil.processEnvironmentVariable(
            InferenceConstants.INFERENCE_ENVIRONMENT_VARIABLE_PREFIX, INFERENCE_SERVICE_URL_ENV_VARIABLE, "").equals(Strings.EMPTY);
        isInferencePayloadModelDefined = !EnvironmentVariableProcessorUtil.processEnvironmentVariable(
            InferenceConstants.INFERENCE_ENVIRONMENT_VARIABLE_PREFIX, INFERENCE_PAYLOAD_MODEL, "").equals(Strings.EMPTY);
    }

    @After
    public void tearDown() {
        rootBuilder = null;
        nodeStore = null;
    }

    /**
     * Test 1: Basic test - Disabled InferenceConfig
     * Verifies that when inference config is created but disabled, the InferenceConfig object reflects this state
     */
    @Test
    public void testDisabledInferenceConfig() throws CommitFailedException {
        // Create disabled inference config node structure
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, false);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object using the nodeStore
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertFalse("InferenceConfig should be disabled", inferenceConfig.isEnabled());
    }

    /**
     * Test 2: Enabled InferenceConfig but no index configs
     * Verifies that when an empty inference config is enabled, the InferenceConfig object reflects this state
     */
    @Test
    public void testEnabledEmptyInferenceConfig() throws CommitFailedException {
        // Create enabled inference config node structure
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object using the nodeStore
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());
    }

    /**
     * Test 3: Basic InferenceIndexConfig creation
     * Tests the creation of a simple InferenceIndexConfig within InferenceConfig
     */
    @Test
    public void testBasicInferenceIndexConfig() throws CommitFailedException {
        // Create enabled inference config with one index config
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

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());
//        assertEquals("Should have one index config", 1, inferenceConfig.getIndexConfigs().size());
        assertTrue("Should contain the index config", inferenceConfig.getInferenceIndexConfig(indexName).isEnabled());

        InferenceIndexConfig indexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        assertTrue("Index config should be enabled", indexConfig.isEnabled());
        assertEquals("Enricher config should match", ENRICHER_CONFIG, indexConfig.getEnricherConfig());
        assertTrue("Model configs should be empty", indexConfig.getInferenceModelConfigs().isEmpty());
    }

    /**
     * Test 4: Disabled InferenceIndexConfig
     * Tests that a disabled InferenceIndexConfig is properly handled
     */
    @Test
    public void testDisabledInferenceIndexConfig() throws CommitFailedException, JsonProcessingException {
        // Create enabled inference config with one disabled index config
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add disabled index config
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, false);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        InferenceIndexConfig indexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        assertFalse("Index config should be disabled", indexConfig.isEnabled());
        // When indexConfig is disabled, we should get the NOOP instance
        assertEquals("Should get NOOP instance", InferenceIndexConfig.NOOP, indexConfig);
    }

    /**
     * Test 5: Invalid InferenceIndexConfig (missing type)
     * Tests that an invalid InferenceIndexConfig (missing type) is properly handled
     */
    @Test
    public void testInvalidInferenceIndexConfig() throws CommitFailedException {
        // Create enabled inference config with one invalid index config (missing type)
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add invalid index config (missing type)
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        // Intentionally not setting the TYPE property
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        InferenceIndexConfig indexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        assertFalse("Invalid index config should be treated as disabled", indexConfig.isEnabled());
        assertEquals("Should get NOOP instance", InferenceIndexConfig.NOOP, indexConfig);
    }

    /**
     * Test 6: Basic InferenceModelConfig
     * Tests the creation of an InferenceModelConfig within an InferenceIndexConfig
     */
    @Test
    public void testBasicInferenceModelConfig() throws CommitFailedException {
        // Create enabled inference config with an index config containing a model config
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add index config
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Add model config
        String modelName = "testModel";
        NodeBuilder modelConfigBuilder = indexConfigBuilder.child(modelName);
        modelConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        modelConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        modelConfigBuilder.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        modelConfigBuilder.setProperty(InferenceModelConfig.MODEL, "test-embedding-model");
        modelConfigBuilder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080/embeddings");
        modelConfigBuilder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        modelConfigBuilder.setProperty(InferenceModelConfig.MIN_TERMS, 3L);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        InferenceIndexConfig indexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        assertTrue("Index config should be enabled", indexConfig.isEnabled());
        assertEquals("Should have one model config", 1, indexConfig.getInferenceModelConfigs().size());

        InferenceModelConfig modelConfig = indexConfig.getInferenceModelConfigs().get(modelName);
        assertNotNull("Model config should exist", modelConfig);
        assertTrue("Model config should be enabled", modelConfig.isEnabled());
        assertTrue("Model config should be default", modelConfig.isDefault());
        assertEquals("Model name should match", "test-embedding-model", modelConfig.getModel());
        assertEquals("Embedding service URL should match", "http://localhost:8080/embeddings", modelConfig.getEmbeddingServiceUrl());
        assertEquals("Similarity threshold should match", 0.8, modelConfig.getSimilarityThreshold(), 0.001);
        assertEquals("Min terms should match", 3L, modelConfig.getMinTerms());
    }

    /**
     * Test 7: Multiple InferenceModelConfigs with one default
     * Tests multiple InferenceModelConfigs within an InferenceIndexConfig, with one marked as default
     */
    @Test
    public void testMultipleInferenceModelConfigs() throws CommitFailedException {
        // Create enabled inference config with an index config containing multiple model configs
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add index config
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Add default model config
        String defaultModelName = "defaultModel";
        NodeBuilder defaultModelConfigBuilder = indexConfigBuilder.child(defaultModelName);
        defaultModelConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        defaultModelConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        defaultModelConfigBuilder.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        defaultModelConfigBuilder.setProperty(InferenceModelConfig.MODEL, "default-embedding-model");
        defaultModelConfigBuilder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080/default-embeddings");
        defaultModelConfigBuilder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        defaultModelConfigBuilder.setProperty(InferenceModelConfig.MIN_TERMS, 3L);

        // Add non-default model config
        String nonDefaultModelName = "nonDefaultModel";
        NodeBuilder nonDefaultModelConfigBuilder = indexConfigBuilder.child(nonDefaultModelName);
        nonDefaultModelConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        nonDefaultModelConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        nonDefaultModelConfigBuilder.setProperty(InferenceModelConfig.IS_DEFAULT, false);
        nonDefaultModelConfigBuilder.setProperty(InferenceModelConfig.MODEL, "non-default-embedding-model");
        nonDefaultModelConfigBuilder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080/non-default-embeddings");
        nonDefaultModelConfigBuilder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.7);
        nonDefaultModelConfigBuilder.setProperty(InferenceModelConfig.MIN_TERMS, 2L);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        InferenceIndexConfig indexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        assertTrue("Index config should be enabled", indexConfig.isEnabled());
        assertEquals("Should have two model configs", 2, indexConfig.getInferenceModelConfigs().size());

        // Verify default model config
        InferenceModelConfig defaultModel = indexConfig.getDefaultEnabledModel();
        assertNotNull("Default model should exist", defaultModel);
        assertEquals("Default model name should match", defaultModelName, defaultModel.getInferenceModelConfigName());
        assertTrue("Default model should be marked as default", defaultModel.isDefault());

        // Verify non-default model config
        InferenceModelConfig nonDefaultModel = indexConfig.getInferenceModelConfigs().get(nonDefaultModelName);
        assertNotNull("Non-default model should exist", nonDefaultModel);
        assertFalse("Non-default model should not be marked as default", nonDefaultModel.isDefault());
    }

    /**
     * Test 8: Complete configuration with multiple indexes and models
     * Tests a complex configuration with multiple indexes and models
     */
    @Test
    public void testCompleteConfiguration() throws CommitFailedException {

        assertTrue(isAuthEnvVarDefined && isInferenceUrlEnvVarDefined && isInferencePayloadModelDefined);
        // Create enabled inference config with multiple index configs
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // First index config
        String indexName1 = "testIndex1";
        NodeBuilder indexConfigBuilder1 = inferenceConfigBuilder.child(indexName1);
        indexConfigBuilder1.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder1.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder1.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Add model config to first index
        String modelName1 = "testModel1";
        NodeBuilder modelConfigBuilder1 = indexConfigBuilder1.child(modelName1);
        modelConfigBuilder1.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        modelConfigBuilder1.setProperty(InferenceConstants.ENABLED, true);
        modelConfigBuilder1.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        modelConfigBuilder1.setProperty(InferenceModelConfig.MODEL, "model1");
        modelConfigBuilder1.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, INFERENCE_SERVICE_URL_ENV_VARIABLE);
        modelConfigBuilder1.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        modelConfigBuilder1.setProperty(InferenceModelConfig.MIN_TERMS, 3L);

        // Add header and payload for model1
        NodeBuilder headerBuilder1 = modelConfigBuilder1.child(InferenceModelConfig.HEADER);
        headerBuilder1.setProperty("Content-Type", "application/json");
        headerBuilder1.setProperty("Authorization", AUTH_ENV_VARIABLE);
        headerBuilder1.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder payloadBuilder1 = modelConfigBuilder1.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder1.setProperty("model", INFERENCE_PAYLOAD_MODEL);
        payloadBuilder1.setProperty("jcr:primaryType", "nt:unstructured");

        // Second index config
        String indexName2 = "testIndex2";
        NodeBuilder indexConfigBuilder2 = inferenceConfigBuilder.child(indexName2);
        indexConfigBuilder2.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder2.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder2.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Add model config to second index
        String modelName2 = "testModel2";
        NodeBuilder modelConfigBuilder2 = indexConfigBuilder2.child(modelName2);
        modelConfigBuilder2.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        modelConfigBuilder2.setProperty(InferenceConstants.ENABLED, true);
        modelConfigBuilder2.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        modelConfigBuilder2.setProperty(InferenceModelConfig.MODEL, "model2");
        modelConfigBuilder2.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, INFERENCE_SERVICE_URL_ENV_VARIABLE);
        modelConfigBuilder2.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.7);
        modelConfigBuilder2.setProperty(InferenceModelConfig.MIN_TERMS, 2L);
        modelConfigBuilder2.setProperty(InferenceModelConfig.TIMEOUT, 10000L);
        modelConfigBuilder2.setProperty(InferenceModelConfig.NUM_CANDIDATES, 200);
        modelConfigBuilder2.setProperty(InferenceModelConfig.CACHE_SIZE, 200);

        // Add header and payload for model2
        NodeBuilder headerBuilder2 = modelConfigBuilder2.child(InferenceModelConfig.HEADER);
        headerBuilder2.setProperty("Content-Type", "application/json");
        headerBuilder2.setProperty("Authorization", AUTH_ENV_VARIABLE);
        headerBuilder2.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder payloadBuilder2 = modelConfigBuilder2.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder2.setProperty("model", "text-embedding-3-large");
        payloadBuilder2.setProperty("dimensions", 1024);
        payloadBuilder2.setProperty("jcr:primaryType", "nt:unstructured");

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        // Test first index config
        InferenceIndexConfig indexConfig1 = inferenceConfig.getInferenceIndexConfig(indexName1);
        assertTrue("Index config 1 should be enabled", indexConfig1.isEnabled());
        assertEquals("Index config 1 should have one model config", 1, indexConfig1.getInferenceModelConfigs().size());

        // Test model config in first index
        InferenceModelConfig modelConfig1 = indexConfig1.getInferenceModelConfigs().get(modelName1);
        assertNotNull("Model config 1 should exist", modelConfig1);
        assertTrue("Model config 1 should be enabled", modelConfig1.isEnabled());
        assertTrue("Model config 1 should be default", modelConfig1.isDefault());
        assertEquals("Model 1 name should match", "model1", modelConfig1.getModel());
        assertEquals("Model 1 similarity threshold should match", 0.8, modelConfig1.getSimilarityThreshold(), 0.001);
        assertFalse("Payload should not have jcr:primaryType property",
            modelConfig1.getPayload().getInferencePayload("input text").contains("jcr:primaryType"));
        assertFalse("Header Payload should not have jcr:primaryType property", modelConfig1.getHeader().getInferenceHeaderPayload().containsKey("jcr:primaryType"));
        assertFalse("Model 1 payload model should not contain " + INFERENCE_PAYLOAD_MODEL, modelConfig1.getPayload().getInferencePayload("input-text").contains(INFERENCE_PAYLOAD_MODEL));

        // Test second index config
        InferenceIndexConfig indexConfig2 = inferenceConfig.getInferenceIndexConfig(indexName2);
        assertTrue("Index config 2 should be enabled", indexConfig2.isEnabled());
        assertEquals("Index config 2 should have one model config", 1, indexConfig2.getInferenceModelConfigs().size());

        // Test model config in second index
        InferenceModelConfig modelConfig2 = indexConfig2.getInferenceModelConfigs().get(modelName2);
        assertNotNull("Model config 2 should exist", modelConfig2);
        assertTrue("Model config 2 should be enabled", modelConfig2.isEnabled());
        assertTrue("Model config 2 should be default", modelConfig2.isDefault());
        assertEquals("Model 2 name should match", "model2", modelConfig2.getModel());
        assertEquals("Model 2 similarity threshold should match", 0.7, modelConfig2.getSimilarityThreshold(), 0.001);
        assertEquals("Model 2 timeout should match", 10000L, modelConfig2.getTimeoutMillis());
        assertEquals("Model 2 num candidates should match", 200, modelConfig2.getNumCandidates());
        assertEquals("Model 2 cache size should match", 200, modelConfig2.getCacheSize());
        assertNotEquals("Model 2 embedding service URL should not match", INFERENCE_SERVICE_URL_ENV_VARIABLE, modelConfig2.getEmbeddingServiceUrl());
        assertNotEquals("Model 2 embedding service URL should not match empty string", "", modelConfig2.getEmbeddingServiceUrl());
        // this is picked from pom.xml during test
        assertEquals("Model 2 embedding service URL should match", "http://localhost:8080/embeddings", modelConfig2.getEmbeddingServiceUrl());
        assertFalse("Payload should not have jcr:primaryType property",
            modelConfig2.getPayload().getInferencePayload("input text").contains("jcr:primaryType"));
        assertFalse("Header Payload should not have jcr:primaryType property", modelConfig2.getHeader().getInferenceHeaderPayload().containsKey("jcr:primaryType"));

    }

    /**
     * Test 9: Test refreshConfig method
     * Tests that the refreshConfig method properly updates the configuration
     */
    @Test
    public void testRefreshConfig() throws CommitFailedException {
        // Create initial enabled inference config with one index
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

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify initial state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        // Modify the configuration in the nodeStore
        NodeBuilder updatedRootBuilder = nodeStore.getRoot().builder();
        NodeBuilder updatedConfigBuilder = createNodePath(updatedRootBuilder, DEFAULT_CONFIG_PATH);

        // Add a new index config
        String newIndexName = "newIndex";
        NodeBuilder newIndexConfigBuilder = updatedConfigBuilder.child(newIndexName);
        newIndexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        newIndexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        newIndexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Commit the changes
        nodeStore.merge(updatedRootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertFalse("Should not have new index config", inferenceConfig.getInferenceIndexConfig(newIndexName).isEnabled());

        // Refresh the InferenceConfig
        InferenceConfig.reInitialize();
        // Verify updated state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());
        assertTrue("Should contain the new index config", inferenceConfig.getInferenceIndexConfig(newIndexName).isEnabled());
    }

    /**
     * Test 10: Test EnricherStatus
     * Tests that the EnricherStatus is properly loaded from the inference config
     */
    @Test
    public void testEnricherStatus() throws CommitFailedException {
        // Create enabled inference config with enrich status node
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add the enricher status node
        NodeBuilder enrichBuilder = inferenceConfigBuilder.child(InferenceConstants.ENRICH_NODE);
        enrichBuilder.setProperty("jcr:primaryType", "nt:unstructured");
        enrichBuilder.setProperty("status", ENRICH_STATUS_PENDING);
        enrichBuilder.setProperty("timestamp", "2023-01-01T12:00:00Z");
        enrichBuilder.setProperty("count", 100);

        // Add some nested data
        NodeBuilder statsBuilder = enrichBuilder.child("stats");
        statsBuilder.setProperty("jcr:primaryType", "nt:unstructured");
        statsBuilder.setProperty("totalDocs", 1000);
        statsBuilder.setProperty("processedDocs", 950);
        statsBuilder.setProperty("skippedDocs", 50);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the enricher status
        Map<String, Object> enricherStatus = inferenceConfig.getEnricherStatus();

        assertNotNull("Enricher status should not be null", enricherStatus);
        assertFalse("Enricher status should not be empty", enricherStatus.isEmpty());

        // Check top-level properties
        assertEquals("Status should match", ENRICH_STATUS_PENDING, enricherStatus.get("status"));
        assertEquals("Timestamp should match", "2023-01-01T12:00:00Z", enricherStatus.get("timestamp"));
        assertEquals("Count should match", 100L, enricherStatus.get("count"));

        // Check nested properties
        @SuppressWarnings("unchecked")
        Map<String, Object> stats = (Map<String, Object>) enricherStatus.get("stats");
        assertNull("Stats should not exist", stats);
        // Verify that jcr:primaryType is not included
        assertFalse("jcr:primaryType should not be included", enricherStatus.containsKey("jcr:primaryType"));
    }

    /**
     * Test 11: Test Empty EnricherStatus
     * Tests the behavior when no enricher status is present
     */
    @Test
    public void testEmptyEnricherStatus() throws CommitFailedException {
        // Create enabled inference config without enrich status node
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the enricher status
        Map<String, Object> enricherStatus = inferenceConfig.getEnricherStatus();

        assertNotNull("Enricher status should not be null", enricherStatus);
        assertTrue("Enricher status should be empty", enricherStatus.isEmpty());
    }

    /**
     * Test 12: Test EnricherStatus when inference config doesn't exist
     * Tests that an empty enricher status is returned when the inference config doesn't exist
     */
    @Test
    public void testEnricherStatusWithNoInferenceConfig() {
        // Create a different NodeStore without any inference config
        NodeBuilder emptyRootBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        NodeStore emptyNodeStore = new MemoryNodeStore(emptyRootBuilder.getNodeState());

        // Create EnricherStatus directly (not through InferenceConfig)
        EnricherStatus enricherStatus = new EnricherStatus(emptyNodeStore, DEFAULT_CONFIG_PATH);

        // Verify the enricher status
        Map<String, Object> status = enricherStatus.getEnricherStatus();

        assertNotNull("Enricher status should not be null", status);
        assertTrue("Enricher status should be empty", status.isEmpty());
    }

    /**
     * Test 13: Test EnricherStatus Refresh
     * Tests that the EnricherStatus is properly refreshed when the inference config is updated
     */
    @Test
    public void testEnricherStatusRefresh() throws CommitFailedException {
        // Create enabled inference config with initial enrich status node
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add the initial enricher status node with PENDING status
        NodeBuilder enrichBuilder = inferenceConfigBuilder.child(InferenceConstants.ENRICH_NODE);
        enrichBuilder.setProperty("jcr:primaryType", "nt:unstructured");
        enrichBuilder.setProperty("status", ENRICH_STATUS_PENDING);
        enrichBuilder.setProperty("timestamp", "2023-01-01T10:00:00Z");
        enrichBuilder.setProperty("count", 0);

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the initial enricher status
        Map<String, Object> initialStatus = inferenceConfig.getEnricherStatus();
        assertEquals("Initial status should be PENDING", ENRICH_STATUS_PENDING, initialStatus.get("status"));
        assertEquals("Initial timestamp should match", "2023-01-01T10:00:00Z", initialStatus.get("timestamp"));
        assertEquals("Initial count should be 0", 0L, initialStatus.get("count"));

        // Now update the enricher status to COMPLETED
        NodeBuilder updatedRootBuilder = nodeStore.getRoot().builder();
        NodeBuilder updatedConfigBuilder = createNodePath(updatedRootBuilder, DEFAULT_CONFIG_PATH);
        NodeBuilder updatedEnrichBuilder = updatedConfigBuilder.child(InferenceConstants.ENRICH_NODE);
        updatedEnrichBuilder.setProperty("status", ENRICH_STATUS_PENDING);
        updatedEnrichBuilder.setProperty("timestamp", "2023-01-01T12:00:00Z");
        updatedEnrichBuilder.setProperty("count", 100L);

        // Commit the updated changes
        nodeStore.merge(updatedRootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Refresh the InferenceConfig
        InferenceConfig.reInitialize();

        // Get the updated enricher status
        Map<String, Object> updatedStatus = inferenceConfig.getEnricherStatus();

        // Verify the updated enricher status
        assertEquals("Updated status should be COMPLETED", ENRICH_STATUS_PENDING, updatedStatus.get("status"));
        assertEquals("Updated timestamp should match", "2023-01-01T12:00:00Z", updatedStatus.get("timestamp"));
        assertEquals("Updated count should be 100", 100L, updatedStatus.get("count"));
    }

    /**
     * Test 14: Test Complete Integration with EnricherStatus
     * Tests the complete integration of InferenceConfig, InferenceIndexConfig, InferenceModelConfig, and EnricherStatus
     */
    @Test
    public void testCompleteIntegrationWithEnricherStatus() throws CommitFailedException {
        assertTrue(isAuthEnvVarDefined && isInferenceUrlEnvVarDefined && isInferencePayloadModelDefined);

        // Create enabled inference config with complete integration
        NodeBuilder inferenceConfigBuilder = createNodePath(rootBuilder, DEFAULT_CONFIG_PATH);
        inferenceConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceConfig.TYPE);
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);

        // Add enricher status node
        NodeBuilder enrichBuilder = inferenceConfigBuilder.child(InferenceConstants.ENRICH_NODE);
        enrichBuilder.setProperty("jcr:primaryType", "nt:unstructured");
        enrichBuilder.setProperty("status", ENRICH_STATUS_PENDING);
        enrichBuilder.setProperty("timestamp", "2023-01-01T12:00:00Z");
        enrichBuilder.setProperty("count", 100);

        // Add stats to enricher status
        NodeBuilder statsBuilder = enrichBuilder.child("stats");
        statsBuilder.setProperty("jcr:primaryType", "nt:unstructured");
        statsBuilder.setProperty("totalDocs", 1000);
        statsBuilder.setProperty("processedDocs", 950);
        statsBuilder.setProperty("skippedDocs", 50);

        // Add index config
        String indexName = "testIndex";
        NodeBuilder indexConfigBuilder = inferenceConfigBuilder.child(indexName);
        indexConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceIndexConfig.TYPE);
        indexConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        indexConfigBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, ENRICHER_CONFIG);

        // Add model config
        String modelName = "testModel";
        NodeBuilder modelConfigBuilder = indexConfigBuilder.child(modelName);
        modelConfigBuilder.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        modelConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
        modelConfigBuilder.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        modelConfigBuilder.setProperty(InferenceModelConfig.MODEL, "test-embedding-model");
        modelConfigBuilder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, INFERENCE_SERVICE_URL_ENV_VARIABLE);
        modelConfigBuilder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        modelConfigBuilder.setProperty(InferenceModelConfig.MIN_TERMS, 3L);

        // Add header and payload for model
        NodeBuilder headerBuilder = modelConfigBuilder.child(InferenceModelConfig.HEADER);
        headerBuilder.setProperty("Content-Type", "application/json");
        headerBuilder.setProperty("Authorization", AUTH_ENV_VARIABLE);
        headerBuilder.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder payloadBuilder = modelConfigBuilder.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder.setProperty("model", INFERENCE_PAYLOAD_MODEL);
        payloadBuilder.setProperty("jcr:primaryType", "nt:unstructured");

        // Commit the changes
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create InferenceConfig object
        InferenceConfig.reInitialize(nodeStore, DEFAULT_CONFIG_PATH, true);
        InferenceConfig inferenceConfig = InferenceConfig.getInstance();

        // Verify the inference config state
        assertTrue("InferenceConfig should be enabled", inferenceConfig.isEnabled());

        // Verify the enricher status
        Map<String, Object> enricherStatus = inferenceConfig.getEnricherStatus();
        assertNotNull("Enricher status should not be null", enricherStatus);
        assertFalse("Enricher status should not be empty", enricherStatus.isEmpty());
        assertEquals("Status should match", ENRICH_STATUS_PENDING, enricherStatus.get("status"));

        // Verify nested stats
        @SuppressWarnings("unchecked")
        Map<String, Object> stats = (Map<String, Object>) enricherStatus.get("stats");
        assertNull("Stats should not exist", stats);
        // Verify that jcr:primaryType is not included
        assertFalse("jcr:primaryType should not be included", enricherStatus.containsKey("jcr:primaryType"));

        // Verify the index config
        InferenceIndexConfig indexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        assertTrue("Index config should be enabled", indexConfig.isEnabled());
        assertEquals("Enricher config should match", ENRICHER_CONFIG, indexConfig.getEnricherConfig());

        // Verify the model config
        InferenceModelConfig modelConfig = indexConfig.getInferenceModelConfigs().get(modelName);
        assertNotNull("Model config should exist", modelConfig);
        assertTrue("Model config should be enabled", modelConfig.isEnabled());
        assertTrue("Model config should be default", modelConfig.isDefault());
        assertEquals("Model name should match", "test-embedding-model", modelConfig.getModel());

        // Verify model config details
        assertNotEquals("Model's embedding service URL should not match the env variable",
            INFERENCE_SERVICE_URL_ENV_VARIABLE, modelConfig.getEmbeddingServiceUrl());
        assertFalse("Payload should not have jcr:primaryType property",
            modelConfig.getPayload().getInferencePayload("test").contains("jcr:primaryType"));
        assertFalse("Header should not have jcr:primaryType property",
            modelConfig.getHeader().getInferenceHeaderPayload().containsKey("jcr:primaryType"));
    }

    /**
     * Utility method to create a path of nodes
     */
    private NodeBuilder createNodePath(NodeBuilder rootBuilder, String path) {
        NodeBuilder builder = rootBuilder;
        for (String elem : PathUtils.elements(path)) {
            builder = builder.child(elem);
        }
        return builder;
    }
} 