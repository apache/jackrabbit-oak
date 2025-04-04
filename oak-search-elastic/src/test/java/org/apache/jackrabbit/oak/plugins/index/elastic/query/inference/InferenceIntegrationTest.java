package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.commons.logging.Log;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class InferenceIntegrationTest {
    Logger Log = LoggerFactory.getLogger(InferenceIntegrationTest.class);
    private NodeBuilder rootBuilder;
    private NodeBuilder inferenceConfigBuilder;

    @Before
    public void setup() {
        rootBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        // Create inference config structure
        inferenceConfigBuilder = rootBuilder.child("oak:index").child("inferenceConfig");
        inferenceConfigBuilder.setProperty(InferenceConstants.ENABLED, true);
    }

    @Test
    public void testCompleteInferenceConfiguration() {
        // Setup index configuration
        NodeBuilder indexBuilder = inferenceConfigBuilder.child("testIndex");
        indexBuilder.setProperty("type", InferenceConstants.INFERENCE_INDEX_CONFIG);
        indexBuilder.setProperty(InferenceConstants.ENRICHER_CONFIG, "{\"enricher\": \"config\"}");

        // Add model configuration
        NodeBuilder modelBuilder = indexBuilder.child("defaultModel");
        modelBuilder.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        setupModelConfiguration(modelBuilder);

        // Create the full configuration
        InferenceConfig inferenceConfig = new InferenceConfig(inferenceConfigBuilder.getNodeState());

        // Verify top-level config
        assertTrue(inferenceConfig.isEnabled());
        assertEquals(1, inferenceConfig.getIndexConfigs().size());

        // Verify index config
        InferenceIndexConfig indexConfig = inferenceConfig.getIndexConfigs().get("testIndex");
        assertNotNull(indexConfig);
        assertEquals("{\"enricher\": \"config\"}", indexConfig.getEnricherConfig());

        // Verify model config
        InferenceModelConfig modelConfig = indexConfig.getDefaultModel();
        assertNotNull(modelConfig);
        assertTrue(modelConfig.isEnabled());
        assertTrue(modelConfig.isDefault());
        assertEquals("test-model", modelConfig.getModel());
        assertEquals(0.8, modelConfig.getSimilarityThreshold(), 0.001);

        // Test inference payload generation
        String testText = "sample text";
        String payload = modelConfig.getPayload().getInferencePayload(testText);
        Log.info("Generated payload: " + payload);
        Log.info("Inference Config: " + inferenceConfig);
        assertNotNull(payload);
    }

    @Test
    public void testMultipleModelsConfiguration() {
        // Setup index with multiple models
        NodeBuilder indexBuilder = inferenceConfigBuilder.child("testIndex");
        indexBuilder.setProperty("type", InferenceConstants.INFERENCE_INDEX_CONFIG);
        
        // Add two models - one default, one not
        NodeBuilder model1Builder = indexBuilder.child("model1");
        model1Builder.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        setupModelConfiguration(model1Builder, true);

        NodeBuilder model2Builder = indexBuilder.child("model2");
        model2Builder.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        setupModelConfiguration(model2Builder, false);

        // Create and verify configuration
        InferenceConfig inferenceConfig = new InferenceConfig(inferenceConfigBuilder.getNodeState());
        InferenceIndexConfig indexConfig = inferenceConfig.getIndexConfigs().get("testIndex");
        
        assertEquals(2, indexConfig.getInferenceModels().size());
        assertNotNull(indexConfig.getDefaultModel());
        assertEquals("model1", indexConfig.getInferenceModels()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().isDefault())
            .map(e -> e.getKey())
            .findFirst()
            .orElse(null));
    }

    private void setupModelConfiguration(NodeBuilder modelBuilder) {
        setupModelConfiguration(modelBuilder, true);
    }

    private void setupModelConfiguration(NodeBuilder modelBuilder, boolean isDefault) {
        modelBuilder.setProperty(InferenceModelConfig.MODEL, "test-model");
        modelBuilder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080");
        modelBuilder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        modelBuilder.setProperty(InferenceModelConfig.MIN_TERMS, 3L);
        modelBuilder.setProperty(InferenceModelConfig.IS_DEFAULT, isDefault);
        modelBuilder.setProperty(InferenceModelConfig.ENABLED, true);

        // Setup header configuration
        NodeBuilder headerBuilder = modelBuilder.child(InferenceModelConfig.HEADER);
        headerBuilder.setProperty("headerKey", "headerValue");

        // Setup payload configuration 
        NodeBuilder payloadBuilder = modelBuilder.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder.setProperty("textKey", "text");
    }
} 