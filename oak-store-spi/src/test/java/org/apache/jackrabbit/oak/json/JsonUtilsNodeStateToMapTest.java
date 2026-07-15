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
package org.apache.jackrabbit.oak.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class JsonUtilsNodeStateToMapTest {
    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testEmptyNodeStateToJson() throws JsonProcessingException {
        // Create an empty node state
        NodeStore nodeStore = new MemoryNodeStore();
        NodeState emptyNodeState = nodeStore.getRoot().builder().getNodeState();
        
        // Convert to JSON
        String json = JsonUtils.nodeStateToJson(emptyNodeState, -1);
        
        // Verify the JSON is well-formed and represents an empty object
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertTrue(jsonNode.isObject());
        Assert.assertEquals(0, jsonNode.size());
    }
    
    @Test
    public void testSimpleNodeStateToJson() throws JsonProcessingException {
        // Create a simple node state with one property
        NodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("simpleProperty", "simple value");
        NodeState simpleNodeState = builder.getNodeState();
        
        // Convert to JSON
        String json = JsonUtils.nodeStateToJson(simpleNodeState, -1);
        
        // Verify the JSON
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertTrue(jsonNode.isObject());
        Assert.assertEquals(1, jsonNode.size());
        Assert.assertEquals("simple value", jsonNode.get("simpleProperty").asText());
    }
    
    @Test
    public void testNodeStateWithMultiplePropertyTypes() throws JsonProcessingException {
        // Create a node state with various property types
        NodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("stringProp", "string value");
        builder.setProperty("booleanProp", true);
        builder.setProperty("longProp", 1234L);
        builder.setProperty("doubleProp", 12.34);
        
        NodeState multiPropNodeState = builder.getNodeState();
        
        // Convert to JSON
        String json = JsonUtils.nodeStateToJson(multiPropNodeState, -1);
        
        // Verify the JSON
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertTrue(jsonNode.isObject());
        Assert.assertEquals(4, jsonNode.size());
        Assert.assertEquals("string value", jsonNode.get("stringProp").asText());
        Assert.assertTrue(jsonNode.get("booleanProp").asBoolean());
        Assert.assertEquals(1234, jsonNode.get("longProp").asLong());
        Assert.assertEquals(12.34, jsonNode.get("doubleProp").asDouble(), 0.001);
    }
    
    @Test
    public void testNodeStateWithArrayProperties() throws JsonProcessingException {
        // Create a node state with array properties
        NodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("stringArray", Arrays.asList("value1", "value2", "value3"), Type.STRINGS);
        builder.setProperty("longArray", Arrays.asList(1L, 2L, 3L), Type.LONGS);
        builder.setProperty("booleanArray", Arrays.asList(true, false, true), Type.BOOLEANS);
        
        NodeState arrayPropNodeState = builder.getNodeState();
        
        // Convert to JSON
        String json = JsonUtils.nodeStateToJson(arrayPropNodeState, -1);
        
        // Verify the JSON
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertTrue(jsonNode.isObject());
        Assert.assertTrue(jsonNode.get("stringArray").isArray());
        Assert.assertEquals(3, jsonNode.get("stringArray").size());
        Assert.assertEquals("value2", jsonNode.get("stringArray").get(1).asText());
        
        Assert.assertTrue(jsonNode.get("longArray").isArray());
        Assert.assertEquals(3, jsonNode.get("longArray").size());
        Assert.assertEquals(2, jsonNode.get("longArray").get(1).asLong());
        
        Assert.assertTrue(jsonNode.get("booleanArray").isArray());
        Assert.assertEquals(3, jsonNode.get("booleanArray").size());
        Assert.assertFalse(jsonNode.get("booleanArray").get(1).asBoolean());
    }
    
    @Test
    public void testNodeStateToJsonWithDepthLimit() throws JsonProcessingException {
        NodeStore nodeStore = new MemoryNodeStore();
        NodeState root = nodeStore.getRoot();
        NodeBuilder nodeBuilder = root.builder();
        nodeBuilder.setChildNode("childNode1")
            .setProperty("testProperty", "testValue")
            .setProperty("jcr:primaryType", "nt:base")
            .setProperty("jcr:created", "2023-10-01T00:00:00.000Z");
        nodeBuilder.setChildNode("childNode2")
            .setProperty("testProperty2", "testValue2")
            .setChildNode("childNode3").setProperty("testProperty3", "testValue3");

        NodeState testNodeState = nodeBuilder.getNodeState();
        
        // Test with depth 0 (only the root node, no children)
        String json0 = JsonUtils.nodeStateToJson(testNodeState, 0);
        JsonNode jsonNode0 = mapper.readTree(json0);
        Assert.assertTrue(jsonNode0.isObject());
        Assert.assertEquals(2, jsonNode0.size());
        
        // Test with depth 1 (root + immediate children, but not grandchildren)
        String json1 = JsonUtils.nodeStateToJson(testNodeState, 1);
        JsonNode jsonNode1 = mapper.readTree(json1);
        Assert.assertTrue(jsonNode1.isObject());
        Assert.assertTrue(jsonNode1.has("childNode1"));
        Assert.assertTrue(jsonNode1.has("childNode2"));
        Assert.assertTrue(jsonNode1.get("childNode2").has("childNode3"));
        Assert.assertEquals(0, jsonNode1.get("childNode2").get("childNode3").size());

        // Convert to JSON with unlimited depth
        String json = JsonUtils.nodeStateToJson(testNodeState, -1);

        // Verify the JSON structure
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertTrue(jsonNode.isObject());
        Assert.assertTrue(jsonNode.has("childNode1"));
        Assert.assertTrue(jsonNode.has("childNode2"));

        JsonNode childNode1 = jsonNode.get("childNode1");
        Assert.assertEquals("testValue", childNode1.get("testProperty").asText());
        Assert.assertEquals("nt:base", childNode1.get("jcr:primaryType").asText());

        JsonNode childNode2 = jsonNode.get("childNode2");
        Assert.assertEquals("testValue2", childNode2.get("testProperty2").asText());
        Assert.assertTrue(childNode2.has("childNode3"));
        Assert.assertEquals("testValue3", childNode2.get("childNode3").get("testProperty3").asText());
    }
    
    @Test
    public void testComplexNodeStateWithNestedStructure() throws JsonProcessingException {
        // Create a complex node structure
        NodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder builder = nodeStore.getRoot().builder();


        // Root level properties
        builder.setProperty("rootProp1", "root value");
        builder.setProperty("rootProp2", 100);

        // Level 1: Category node
        NodeBuilder categoryBuilder = builder.setChildNode("categories");
        categoryBuilder.setProperty("description", "Product categories");

        // Level 2: Individual categories
        NodeBuilder electronics = categoryBuilder.setChildNode("electronics");
        electronics.setProperty("displayName", "Electronics");
        electronics.setProperty("active", true);

        NodeBuilder clothing = categoryBuilder.setChildNode("clothing");
        clothing.setProperty("displayName", "Clothing");
        clothing.setProperty("active", false);

        // Level 3: Subcategories
        NodeBuilder phones = electronics.setChildNode("phones");
        phones.setProperty("displayName", "Mobile Phones");
        phones.setProperty("items", Arrays.asList(1001L, 1002L, 1003L), Type.LONGS);

        NodeBuilder laptops = electronics.setChildNode("laptops");
        laptops.setProperty("displayName", "Laptops");
        laptops.setProperty("featured", true);

        NodeState complexNodeState = builder.getNodeState();
        // Convert to JSON
        String json = JsonUtils.nodeStateToJson(complexNodeState, -1);
        // Verify the complex JSON structure
        JsonNode jsonNode = mapper.readTree(json);
        
        // Root level verification
        Assert.assertEquals("root value", jsonNode.get("rootProp1").asText());
        Assert.assertEquals(100, jsonNode.get("rootProp2").asInt());
        
        // Level 1 verification
        JsonNode categoriesJsonNode = jsonNode.get("categories");
        Assert.assertNotNull(categoriesJsonNode);
        Assert.assertEquals("Product categories", categoriesJsonNode.get("description").asText());
        
        // Level 2 verification
        JsonNode electronicsJsonNode = categoriesJsonNode.get("electronics");
        Assert.assertNotNull(electronicsJsonNode);
        Assert.assertEquals("Electronics", electronicsJsonNode.get("displayName").asText());
        Assert.assertTrue(electronicsJsonNode.get("active").asBoolean());
        
        JsonNode clothingJsonNode = categoriesJsonNode.get("clothing");
        Assert.assertNotNull(clothingJsonNode);
        Assert.assertEquals("Clothing", clothingJsonNode.get("displayName").asText());
        Assert.assertFalse(clothingJsonNode.get("active").asBoolean());
        
        // Level 3 verification
        JsonNode phonesJsonNode = electronicsJsonNode.get("phones");
        Assert.assertNotNull(phonesJsonNode);
        Assert.assertEquals("Mobile Phones", phonesJsonNode.get("displayName").asText());
        Assert.assertTrue(phonesJsonNode.get("items").isArray());
        Assert.assertEquals(3, phonesJsonNode.get("items").size());
        Assert.assertEquals(1002, phonesJsonNode.get("items").get(1).asInt());
        
        JsonNode laptopsJsonNode = electronicsJsonNode.get("laptops");
        Assert.assertNotNull(laptopsJsonNode);
        Assert.assertEquals("Laptops", laptopsJsonNode.get("displayName").asText());
        Assert.assertTrue(laptopsJsonNode.get("featured").asBoolean());
    }
}
