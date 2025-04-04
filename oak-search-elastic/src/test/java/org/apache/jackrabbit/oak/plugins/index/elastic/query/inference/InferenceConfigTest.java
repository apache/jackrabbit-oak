package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import static org.junit.Assert.*;

public class InferenceConfigTest {

    @Test
    public void testEmptyConfig() {
        InferenceConfig config = new InferenceConfig(EmptyNodeState.EMPTY_NODE);
        assertFalse(config.isEnabled());
        assertTrue(config.getIndexConfigs().isEmpty());
    }

    @Test
    public void testEnabledConfig() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty(InferenceConstants.ENABLED, true);
        
        InferenceConfig config = new InferenceConfig(builder.getNodeState());
        assertTrue(config.isEnabled());
        assertTrue(config.getIndexConfigs().isEmpty());
    }

    @Test
    public void testWithValidIndexConfig() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty(InferenceConstants.ENABLED, true);
        
        // Add a valid index config
        NodeBuilder indexBuilder = builder.child("testIndex");
        indexBuilder.setProperty("type", InferenceConstants.INFERENCE_INDEX_CONFIG);
        
        InferenceConfig config = new InferenceConfig(builder.getNodeState());
        assertTrue(config.isEnabled());
        assertEquals(1, config.getIndexConfigs().size());
        assertTrue(config.getIndexConfigs().containsKey("testIndex"));
    }

    @Test
    public void testWithInvalidIndexConfig() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty(InferenceConstants.ENABLED, true);
        
        // Add an invalid index config (missing type property)
        builder.child("testIndex");
        
        InferenceConfig config = new InferenceConfig(builder.getNodeState());
        assertTrue(config.isEnabled());
        assertTrue(config.getIndexConfigs().isEmpty());
    }
} 