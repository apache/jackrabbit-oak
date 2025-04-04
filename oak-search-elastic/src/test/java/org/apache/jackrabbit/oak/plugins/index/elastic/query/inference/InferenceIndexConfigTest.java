package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class InferenceIndexConfigTest {
    private MemoryNodeBuilder builder;

    @Before
    public void setup() {
        builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty(InferenceConstants.ENRICHER_CONFIG, "{\"key\": \"value\"}");
    }

    @Test
    public void testBasicConfig() {
        InferenceIndexConfig config = new InferenceIndexConfig(builder.getNodeState());
        assertEquals("{\"key\": \"value\"}", config.getEnricherConfig());
        assertTrue(config.getInferenceModels().isEmpty());
    }

    @Test
    public void testWithInferenceModel() {
        // Add an inference model config
        NodeBuilder modelBuilder = builder.child("model1");
        modelBuilder.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        setupModelProperties(modelBuilder);
        
        InferenceIndexConfig config = new InferenceIndexConfig(builder.getNodeState());
        assertEquals(1, config.getInferenceModels().size());
        assertTrue(config.getInferenceModels().containsKey("model1"));
    }

    @Test
    public void testDefaultModel() {
        // Add two models, one default and one not
        NodeBuilder model1 = builder.child("model1");
        model1.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        setupModelProperties(model1, true);

        NodeBuilder model2 = builder.child("model2");
        model2.setProperty(InferenceConstants.INFERENCE_CONFIG_TYPE, InferenceConstants.INFERENCE_MODEL_CONFIG);
        setupModelProperties(model2, false);

        InferenceIndexConfig config = new InferenceIndexConfig(builder.getNodeState());
        assertNotNull(config.getDefaultModel());
        assertEquals("test-model", config.getDefaultModel().getModel());
    }

    private void setupModelProperties(NodeBuilder builder) {
        setupModelProperties(builder, false);
    }

    private void setupModelProperties(NodeBuilder builder, boolean isDefault) {
        builder.setProperty(InferenceModelConfig.MODEL, "test-model");
        builder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080");
        builder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        builder.setProperty(InferenceModelConfig.MIN_TERMS, 3L);
        builder.setProperty(InferenceModelConfig.IS_DEFAULT, isDefault);
        builder.setProperty(InferenceModelConfig.ENABLED, true);
        
        // Add required child nodes
        NodeBuilder headerBuilder = builder.child(InferenceModelConfig.HEADER);
        NodeBuilder payloadBuilder = builder.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder.setProperty("textKey", "text");
    }
} 