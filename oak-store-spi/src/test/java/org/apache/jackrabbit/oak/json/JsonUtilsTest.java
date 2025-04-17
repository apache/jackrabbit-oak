package org.apache.jackrabbit.oak.json;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonUtilsTest {

    @Test
    public void testNullInput() {
        assertFalse(JsonUtils.isValidJson(null, false));
        assertFalse(JsonUtils.isValidJson(null, true));
    }

    @Test
    public void testEmptyString() {
        assertFalse(JsonUtils.isValidJson("", false));
        assertFalse(JsonUtils.isValidJson("", true));
    }

    @Test
    public void testValidJsonObject() {
        assertTrue(JsonUtils.isValidJson("{}", false));
        assertTrue(JsonUtils.isValidJson("{\"key\":\"value\"}", false));
        assertTrue(JsonUtils.isValidJson("{\"key\":123}", false));
        assertTrue(JsonUtils.isValidJson("{\"key\":true}", false));
        assertTrue(JsonUtils.isValidJson("{\"key\":null}", false));
        assertTrue(JsonUtils.isValidJson("{\"key\":{\"nested\":\"value\"}}", false));
    }

    @Test
    public void testInvalidJsonObject() {
        assertFalse(JsonUtils.isValidJson("{", false));
        assertFalse(JsonUtils.isValidJson("}", false));
        assertFalse(JsonUtils.isValidJson("{key:value}", false));
        assertFalse(JsonUtils.isValidJson("{\"key\":value}", false));
        assertFalse(JsonUtils.isValidJson("{\"key\":\"value\"", false));
        assertFalse(JsonUtils.isValidJson("{\"key\":\"value\",}", false));
    }

    @Test
    public void testValidJsonArray() {
        assertTrue(JsonUtils.isValidJson("[]", true));
        assertTrue(JsonUtils.isValidJson("[1,2,3]", true));
        assertTrue(JsonUtils.isValidJson("[\"a\",\"b\",\"c\"]", true));
        assertTrue(JsonUtils.isValidJson("[true,false,null]", true));
        assertTrue(JsonUtils.isValidJson("[{\"key\":\"value\"},{\"key2\":123}]", true));
    }

    @Test
    public void testInvalidJsonArray() {
        assertFalse(JsonUtils.isValidJson("[", true));
        assertFalse(JsonUtils.isValidJson("]", true));
        assertFalse(JsonUtils.isValidJson("[1,2,]", true));
        assertFalse(JsonUtils.isValidJson("[1 2 3]", true));
        assertFalse(JsonUtils.isValidJson("[\"unclosed]", true));
    }

    @Test
    public void testArrayNotAllowedWhenFlagIsFalse() {
        assertFalse(JsonUtils.isValidJson("[]", false));
        assertFalse(JsonUtils.isValidJson("[1,2,3]", false));
    }

    @Test
    public void testValidPrimitiveValues() {
        assertFalse(JsonUtils.isValidJson("123", false));
        assertFalse(JsonUtils.isValidJson("\"string\"", false));
        assertFalse(JsonUtils.isValidJson("true", false));
        assertFalse(JsonUtils.isValidJson("false", false));
        assertFalse(JsonUtils.isValidJson("null", false));
    }

    @Test
    public void testInvalidPrimitiveValues() {
        assertFalse(JsonUtils.isValidJson("undefined", false));
        assertFalse(JsonUtils.isValidJson("'string'", false));
        assertFalse(JsonUtils.isValidJson("TRUE", false));
        assertFalse(JsonUtils.isValidJson("False", false));
        assertFalse(JsonUtils.isValidJson("NULL", false));
    }

    @Test
    public void testComplexNestedStructures() {
        assertTrue(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":true}]}", false));
        assertTrue(JsonUtils.isValidJson("[{\"obj\":{}},[[],{}]]", true));
        assertTrue(JsonUtils.isValidJson("{\"a\":{\"b\":{\"c\":{\"d\":null}}}}", false));
        assertFalse(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":Undefined}]}", false));
        assertFalse(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":NaN}]}", false));
        assertFalse(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":Infinity}]}", false));
        assertFalse(JsonUtils.isValidJson("{\"a\":{\"b\":{\"c\":{\"d\":void}}}}", false));
    }

    @Test
    public void testConvertNodeStateToMap() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty("property1", "value1");
        builder.setProperty("property2", 123);
        NodeState nodeState = builder.getNodeState();
        
        Map<String, Object> result = JsonUtils.convertNodeStateToMap(nodeState, 2);
        
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("property1"));
        assertTrue(result.containsKey("property2"));
        assertEquals("value1", result.get("property1"));
        assertEquals(123L, result.get("property2"));
    }

    @Test
    public void testConvertNodeStateToMapWithMaxDepth() {
        MemoryNodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty("property1", "value1");
        NodeState nodeState = builder.getNodeState();
        
        Map<String, Object> result = JsonUtils.convertNodeStateToMap(nodeState, 0);
        assertNotNull(result);

        result = JsonUtils.convertNodeStateToMap(nodeState, -1);
        assertNull(result);
    }

    @Test
    public void testConvertNodeStateToMapWithNestedNodes() {
        NodeBuilder builder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        builder.setProperty("prop1", "val1");
        
        NodeBuilder child = builder.child("child1");
        child.setProperty("childProp1", "childVal1");
        
        NodeBuilder grandChild = child.child("grandChild1");
        grandChild.setProperty("grandChildProp1", "grandChildVal1");

        NodeBuilder greatGrandChild = grandChild.child("greatGrandChild1");
        greatGrandChild.setProperty("greatGrandChildProp1", "greatGrandChildVal1");

        NodeState nodeState = builder.getNodeState();

        // Test with maxDepth = 0 (should return only properties on current node)
        Map<String, Object> result0 = JsonUtils.convertNodeStateToMap(nodeState, 0);
        assertNotNull(result0);
        assertEquals(1, result0.size());
        assertTrue(result0.containsKey("prop1"));
        assertEquals("val1", result0.get("prop1"));
        assertNotNull(result0);
        assertTrue(result0.containsKey("prop1"));
        assertFalse(result0.containsKey("child1"));


        // Test with maxDepth = 1 (should include child1)
        Map<String, Object> result1 = JsonUtils.convertNodeStateToMap(nodeState, 1);
        assertNotNull(result1);
        assertTrue(result1.containsKey("child1"));
        Map<String, Object> childMap1 = (Map<String, Object>) result1.get("child1");
        assertEquals("childVal1", childMap1.get("childProp1"));
        assertNotNull(result1);
        assertTrue(result1.containsKey("prop1"));
        assertTrue(result1.containsKey("child1"));
        assertFalse(childMap1.containsKey("grandChild1"));


        // Test with maxDepth = 2 (should include grandChild1)
        Map<String, Object> result2 = JsonUtils.convertNodeStateToMap(nodeState, 2);
        Map<String, Object>  childMap2 = (Map<String, Object>) result2.get("child1");
        assertTrue(childMap2.containsKey("grandChild1"));
        Map<String, Object> grandChildMap2 = (Map<String, Object>) childMap2.get("grandChild1");
        assertEquals("grandChildVal1", grandChildMap2.get("grandChildProp1"));
        assertNotNull(result2);
        assertTrue(result2.containsKey("prop1"));
        assertTrue(result2.containsKey("child1"));
        assertFalse(grandChildMap2.containsKey("greatGrandChild1"));

        // Test with maxDepth = 3 (should include greatGrandChild1)
        Map<String, Object> result3 = JsonUtils.convertNodeStateToMap(nodeState, 3);
        Map<String, Object>  childMap3 = (Map<String, Object>) result3.get("child1");
        Map<String, Object> grandChildMap3 = (Map<String, Object>) childMap3.get("grandChild1");
        Map<String, Object> greatGrandChildMap3 = (Map<String, Object>) grandChildMap3.get("greatGrandChild1");
        assertEquals("greatGrandChildVal1", greatGrandChildMap3.get("greatGrandChildProp1"));
        assertNotNull(result3);
        assertTrue(result3.containsKey("prop1"));
        assertTrue(result3.containsKey("child1"));

        // Test with maxDepth = -1 (should return all nodes and properties)
        Map<String, Object> resultNeg1 = JsonUtils.convertNodeStateToMap(nodeState, -1);
        Map<String, Object>  childMapNeg1 = (Map<String, Object>) resultNeg1.get("child1");
        Map<String, Object> grandChildMapNeg1 = (Map<String, Object>) childMapNeg1.get("grandChild1");
        assertTrue(grandChildMapNeg1.containsKey("greatGrandChild1"));
        Map<String, Object> greatGrandChildMapNeg1 = (Map<String, Object>) grandChildMapNeg1.get("greatGrandChild1");
        assertEquals("greatGrandChildVal1", greatGrandChildMapNeg1.get("greatGrandChildProp1"));
        assertNotNull(resultNeg1);
        assertTrue(resultNeg1.containsKey("prop1"));
        assertTrue(resultNeg1.containsKey("child1"));


    }
}
