package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class InferenceModelConfigTest {
    private MemoryNodeBuilder builder;

    @Before
    public void setup() {
        builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        // Set required properties
        builder.setProperty(InferenceModelConfig.MODEL, "test-model");
        builder.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, "http://localhost:8080");
        builder.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, 0.8);
        builder.setProperty(InferenceModelConfig.MIN_TERMS, 3L);
        builder.setProperty(InferenceModelConfig.IS_DEFAULT, true);
        builder.setProperty(InferenceModelConfig.ENABLED, true);
        
        // Add header node
        NodeBuilder headerBuilder = builder.child(InferenceModelConfig.HEADER);
        
        // Add inference payload node
        NodeBuilder payloadBuilder = builder.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payloadBuilder.setProperty("textKey", "text");
    }

    @Test
    public void testBasicConfiguration() {
        InferenceModelConfig config = new InferenceModelConfig(builder.getNodeState());
        
        assertEquals("test-model", config.getModel());
        assertEquals("http://localhost:8080", config.getEmbeddingServiceUrl());
        assertEquals(0.8, config.getSimilarityThreshold(), 0.001);
        assertEquals(3L, config.getMinTerms());
        assertTrue(config.isDefault());
        assertTrue(config.isEnabled());
    }

    @Test
    public void testToString() {
        InferenceModelConfig config = new InferenceModelConfig(builder.getNodeState());
        String str = config.toString();
        
        assertTrue(str.contains("test-model"));
        assertTrue(str.contains("http://localhost:8080"));
        assertTrue(str.contains("0.8"));
    }

    @Test
    public void testHeaderConfiguration() {
        InferenceModelConfig config = new InferenceModelConfig(builder.getNodeState());
        assertNotNull(config.getHeader());
    }
} 