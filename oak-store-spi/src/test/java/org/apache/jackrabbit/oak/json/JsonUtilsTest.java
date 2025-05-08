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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonUtilsTest {

    // Helper methods to reduce duplication
    private NodeBuilder createNodeBuilder() {
        return new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
    }

    private <T> void assertPropertyValue(Map<String, Object> result, String propertyName, T expectedValue) {
        assertNotNull(result);
        assertTrue("Missing property: " + propertyName, result.containsKey(propertyName));
        assertEquals("Property value mismatch for " + propertyName, expectedValue, result.get(propertyName));
    }

    private <T> void assertArrayValues(Map<String, Object> result, String propertyName, List<T> expectedValues) {
        assertNotNull(result);
        assertTrue("Missing array property: " + propertyName, result.containsKey(propertyName));

        List<T> resultArray = (List<T>) result.get(propertyName);
        assertNotNull(resultArray);
        assertEquals("Array size mismatch for " + propertyName, expectedValues.size(), resultArray.size());

        for (int i = 0; i < expectedValues.size(); i++) {
            assertEquals("Value mismatch at index " + i + " for " + propertyName,
                         expectedValues.get(i), resultArray.get(i));
        }
    }

    // JSON validation tests
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
        String[] validObjects = {
            "{}",
            "{\"key\":\"value\"}",
            "{\"key\":123}",
            "{\"key\":true}",
            "{\"key\":null}",
            "{\"key\":{\"nested\":\"value\"}}"
        };

        for (String json : validObjects) {
            assertTrue("Should be valid JSON: " + json, JsonUtils.isValidJson(json, false));
        }
    }

    @Test
    public void testInvalidJsonObject() {
        String[] invalidObjects = {
            "{",
            "}",
            "{key:value}",
            "{\"key\":value}",
            "{\"key\":\"value\"",
            "{\"key\":\"value\",}"
        };

        for (String json : invalidObjects) {
            assertFalse("Should be invalid JSON: " + json, JsonUtils.isValidJson(json, false));
        }
    }

    @Test
    public void testValidJsonArray() {
        String[] validArrays = {
            "[]",
            "[1,2,3]",
            "[\"a\",\"b\",\"c\"]",
            "[true,false,null]",
            "[{\"key\":\"value\"},{\"key2\":123}]"
        };

        for (String json : validArrays) {
            assertTrue("Should be valid JSON array: " + json, JsonUtils.isValidJson(json, true));
        }
    }

    @Test
    public void testInvalidJsonArray() {
        String[] invalidArrays = {
            "[",
            "]",
            "[1,2,]",
            "[1 2 3]",
            "[\"unclosed]"
        };

        for (String json : invalidArrays) {
            assertFalse("Should be invalid JSON array: " + json, JsonUtils.isValidJson(json, true));
        }
    }

    @Test
    public void testArrayNotAllowedWhenFlagIsFalse() {
        assertFalse(JsonUtils.isValidJson("[]", false));
        assertFalse(JsonUtils.isValidJson("[1,2,3]", false));
    }

    @Test
    public void testValidPrimitiveValues() {
        String[] primitives = {
            "123",
            "\"string\"",
            "true",
            "false",
            "null"
        };

        for (String json : primitives) {
            assertFalse("Primitive values should not be valid JSON objects: " + json,
                       JsonUtils.isValidJson(json, false));
        }
    }

    @Test
    public void testComplexNestedStructures() {
        // Valid structures
        assertTrue(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":true}]}", false));
        assertTrue(JsonUtils.isValidJson("[{\"obj\":{}},[[],{}]]", true));
        assertTrue(JsonUtils.isValidJson("{\"a\":{\"b\":{\"c\":{\"d\":null}}}}", false));

        // Invalid structures
        assertFalse(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":Undefined}]}", false));
        assertFalse(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":NaN}]}", false));
        assertFalse(JsonUtils.isValidJson("{\"array\":[1,2,{\"nested\":Infinity}]}", false));
        assertFalse(JsonUtils.isValidJson("{\"a\":{\"b\":{\"c\":{\"d\":void}}}}", false));
    }

    // NodeState to Map conversion tests
    @Test
    public void testConvertNodeStateToMap() {
        NodeBuilder builder = createNodeBuilder();
        builder.setProperty("property1", "value1");
        builder.setProperty("property2", 123);

        Map<String, Object> result = JsonUtils.convertNodeStateToMap(builder.getNodeState(), 2, true);

        assertPropertyValue(result, "property1", "value1");
        assertPropertyValue(result, "property2", 123L);
    }

    @Test
    public void testConvertNodeStateToMapWithMaxDepth() {
        NodeBuilder builder = createNodeBuilder();
        builder.setProperty("property1", "value1");

        Map<String, Object> result = JsonUtils.convertNodeStateToMap(builder.getNodeState(), 0, true);
        assertNotNull(result);
    }

    @Test
    public void testConvertNodeStateToMapWithNestedNodes() {
        NodeBuilder builder = createNodeBuilder();
        builder.setProperty("prop1", "val1");

        NodeBuilder child = builder.child("child1");
        child.setProperty("childProp1", "childVal1");

        NodeBuilder grandChild = child.child("grandChild1");
        grandChild.setProperty("grandChildProp1", "grandChildVal1");

        NodeBuilder greatGrandChild = grandChild.child("greatGrandChild1");
        greatGrandChild.setProperty("greatGrandChildProp1", "greatGrandChildVal1");

        NodeState nodeState = builder.getNodeState();

        // Test different depth limits
        int[] depthLimits = {0, 1, 2, 3, -1};
        for (int depth : depthLimits) {
            Map<String, Object> result = JsonUtils.convertNodeStateToMap(nodeState, depth, true);
            assertNotNull(result);
            assertTrue(result.containsKey("prop1"));

            if (depth != 0) {
                assertTrue(result.containsKey("child1"));
                Map<String, Object> childMap = (Map<String, Object>) result.get("child1");
                assertTrue(childMap.containsKey("childProp1"));

                if (depth > 1 || depth == -1) {
                    assertTrue(childMap.containsKey("grandChild1"));
                    Map<String, Object> grandChildMap = (Map<String, Object>) childMap.get("grandChild1");
                    assertTrue(grandChildMap.containsKey("grandChildProp1"));

                    if (depth > 2 || depth == -1) {
                        assertTrue(grandChildMap.containsKey("greatGrandChild1"));
                        Map<String, Object> greatGrandChildMap = (Map<String, Object>) grandChildMap.get("greatGrandChild1");
                        assertTrue(greatGrandChildMap.containsKey("greatGrandChildProp1"));
                    }
                }
            }
        }
    }

    @Test
    public void testConvertNodeStateToMapWithHiddenNodesAndProperties() {
        NodeBuilder builder = createNodeBuilder();

        // Regular property and child
        builder.setProperty("regularProp", "regularValue");
        NodeBuilder regularChild = builder.child("regularChild");
        regularChild.setProperty("childProp", "childValue");

        // Hidden property and child (starts with ":")
        builder.setProperty(":hiddenProp", "hiddenValue");
        NodeBuilder hiddenChild = builder.child(":hiddenChild");
        hiddenChild.setProperty("hiddenChildProp", "hiddenChildValue");

        NodeState nodeState = builder.getNodeState();

        // Test with shouldSerializeHiddenNodesOrProperties = false
        Map<String, Object> resultHidden = JsonUtils.convertNodeStateToMap(nodeState, -1, false);
        assertNotNull(resultHidden);
        assertEquals(2, resultHidden.size());
        assertTrue(resultHidden.containsKey("regularProp"));
        assertTrue(resultHidden.containsKey("regularChild"));
        assertFalse(resultHidden.containsKey(":hiddenProp"));
        assertFalse(resultHidden.containsKey(":hiddenChild"));

        // Test with shouldSerializeHiddenNodesOrProperties = true
        Map<String, Object> resultVisible = JsonUtils.convertNodeStateToMap(nodeState, -1, true);
        assertNotNull(resultVisible);
        assertEquals(4, resultVisible.size());
        assertTrue(resultVisible.containsKey("regularProp"));
        assertTrue(resultVisible.containsKey("regularChild"));
        assertTrue(resultVisible.containsKey(":hiddenProp"));
        assertTrue(resultVisible.containsKey(":hiddenChild"));
    }

    // Type-specific property tests
    @Test
    public void testNodeStateWithDecimalValues() {
        NodeBuilder builder = createNodeBuilder();

        // Single decimal property
        BigDecimal singleDecimal = new BigDecimal("123.456");
        builder.setProperty("decimalProp", singleDecimal);

        // Array of decimal values
        List<BigDecimal> decimalValues = Arrays.asList(
                new BigDecimal("1.1"),
                new BigDecimal("2.2"),
                new BigDecimal("3.3"),
                new BigDecimal("9999.9999")
        );
        builder.setProperty("decimalArray", decimalValues, Type.DECIMALS);

        Map<String, Object> result = JsonUtils.convertNodeStateToMap(builder.getNodeState(), -1, true);

        assertPropertyValue(result, "decimalProp", singleDecimal);
        assertArrayValues(result, "decimalArray", decimalValues);
    }

    @Test
    public void testNodeStateWithDateValues() {
        NodeBuilder builder = createNodeBuilder();

        // Single date property
        Calendar date1 = Calendar.getInstance();
        builder.setProperty("dateProp", date1);

        Map<String, Object> result = JsonUtils.convertNodeStateToMap(builder.getNodeState(), -1, true);

        assertTrue(result.containsKey("dateProp"));

        // Date property should be a String (ISO formatted date)
        Object dateValue = result.get("dateProp");
        assertTrue("Date value is of type " + dateValue.getClass().getName(),
                  dateValue instanceof String);
        // Verify it looks like an ISO date string
        String dateStr = (String) dateValue;
        assertTrue("Date value should be in ISO format: " + dateStr,
                dateStr.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
    }

    @Test
    public void testNodeStateWithDateArrayValues() {
        NodeBuilder builder = createNodeBuilder();

        // String ISO dates for array
        List<String> dateStrings = Arrays.asList(
            "2021-01-01T10:30:00.000Z",
            "2022-02-02T12:45:00.000Z",
            "2023-03-03T14:15:00.000Z"
        );

        builder.setProperty("dateStringArray", dateStrings, Type.DATES);

        Map<String, Object> result = JsonUtils.convertNodeStateToMap(builder.getNodeState(), -1, true);

        assertTrue(result.containsKey("dateStringArray"));

        // Check date array contains String objects (ISO format dates)
        List<?> resultArray = (List<?>) result.get("dateStringArray");
        assertNotNull(resultArray);
        assertEquals(3, resultArray.size());

        for (Object date : resultArray) {
            assertTrue("Date array element is of type " + date.getClass().getName(),
                     date instanceof String);
            String dateStr = (String) date;
            assertTrue("Date array value should be in ISO format: " + dateStr,
                    dateStr.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*"));
        }
    }

    @Test
    public void testNodeStateWithPrimitiveValues() {
        NodeBuilder builder = createNodeBuilder();

        // Boolean properties
        builder.setProperty("boolTrue", true);
        builder.setProperty("boolFalse", false);
        List<Boolean> booleanValues = Arrays.asList(true, false, true, true, false);
        builder.setProperty("booleanArray", booleanValues, Type.BOOLEANS);

        // Double properties
        builder.setProperty("doubleValue", 123.456);
        builder.setProperty("doubleZero", 0.0);
        builder.setProperty("doubleNegative", -987.654);
        List<Double> doubleValues = Arrays.asList(1.1, 2.2, 3.3, -4.4, 0.0);
        builder.setProperty("doubleArray", doubleValues, Type.DOUBLES);

        // Long properties
        builder.setProperty("longValue", 1234567890L);
        builder.setProperty("longZero", 0L);
        builder.setProperty("longNegative", -9876543210L);
        builder.setProperty("longMax", Long.MAX_VALUE);
        builder.setProperty("longMin", Long.MIN_VALUE);
        List<Long> longValues = Arrays.asList(11L, 22L, 33L, -44L, 0L);
        builder.setProperty("longArray", longValues, Type.LONGS);

        // String array
        List<String> mixedStrings = Arrays.asList("string", "123", "true");
        builder.setProperty("mixedArray", mixedStrings, Type.STRINGS);

        Map<String, Object> result = JsonUtils.convertNodeStateToMap(builder.getNodeState(), -1, true);

        // Check boolean values
        assertPropertyValue(result, "boolTrue", true);
        assertPropertyValue(result, "boolFalse", false);
        assertArrayValues(result, "booleanArray", booleanValues);

        // Check double values
        assertEquals(123.456, (Double) result.get("doubleValue"), 0.0001);
        assertEquals(0.0, (Double) result.get("doubleZero"), 0.0001);
        assertEquals(-987.654, (Double) result.get("doubleNegative"), 0.0001);

        List<Double> doubleResultArray = (List<Double>) result.get("doubleArray");
        for (int i = 0; i < doubleValues.size(); i++) {
            assertEquals(doubleValues.get(i), doubleResultArray.get(i), 0.0001);
        }

        // Check long values
        assertPropertyValue(result, "longValue", 1234567890L);
        assertPropertyValue(result, "longZero", 0L);
        assertPropertyValue(result, "longNegative", -9876543210L);
        assertPropertyValue(result, "longMax", Long.MAX_VALUE);
        assertPropertyValue(result, "longMin", Long.MIN_VALUE);
        assertArrayValues(result, "longArray", longValues);

        // Check string array
        assertArrayValues(result, "mixedArray", mixedStrings);
    }

    @Test
    public void testNodeStateToJson() {
        try {
            NodeBuilder builder = createNodeBuilder();

            // Add various types of properties
            builder.setProperty("string", "string value");
            builder.setProperty("long", 12345L);
            builder.setProperty("double", 123.456);
            builder.setProperty("boolean", true);
            builder.setProperty("decimal", new BigDecimal("987.654"));

            // Add array properties
            builder.setProperty("stringArray", Arrays.asList("value1", "value2", "value3"), Type.STRINGS);
            builder.setProperty("longArray", Arrays.asList(1L, 2L, 3L), Type.LONGS);
            builder.setProperty("doubleArray", Arrays.asList(1.1, 2.2, 3.3), Type.DOUBLES);

            // Add a child node
            NodeBuilder child = builder.child("child");
            child.setProperty("childProp", "child value");

            NodeState nodeState = builder.getNodeState();

            String json = JsonUtils.nodeStateToJson(nodeState, -1);

            // Basic verification that the JSON contains expected values
            assertTrue(json.contains("\"string\" : \"string value\""));
            assertTrue(json.contains("\"long\" : 12345"));
            assertTrue(json.contains("\"double\" : 123.456"));
            assertTrue(json.contains("\"boolean\" : true"));

            // Verify array format
            assertTrue(json.contains("\"stringArray\" : [ \"value1\", \"value2\", \"value3\" ]"));
            assertTrue(json.contains("\"longArray\" : [ 1, 2, 3 ]"));

            // Verify child node
            assertTrue(json.contains("\"child\" : {"));
            assertTrue(json.contains("\"childProp\" : \"child value\""));

            // Test with limited depth
            String jsonLimitedDepth = JsonUtils.nodeStateToJson(nodeState, 0);
            assertTrue(jsonLimitedDepth.contains("\"string\" : \"string value\""));
            assertFalse(jsonLimitedDepth.contains("\"childProp\" : \"child value\""));

        } catch (Exception e) {
            fail("Exception thrown during test: " + e.getMessage());
        }
    }
}
