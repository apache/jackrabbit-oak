package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.junit.Test;
import static org.junit.Assert.*;

public class InferencePayloadTest {

    @Test
    public void testInferencePayload() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty("textKey", "content");
        
        InferencePayload payload = new InferencePayload(builder.getNodeState());
        String result = payload.getInferencePayload("test content");
        
        assertNotNull(result);
        assertTrue(result.contains("test content"));
    }

    @Test
    public void testEmptyText() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty("textKey", "content");
        
        InferencePayload payload = new InferencePayload(builder.getNodeState());
        String result = payload.getInferencePayload("");
        
        assertNotNull(result);
        assertTrue(result.contains("content"));
    }
} 