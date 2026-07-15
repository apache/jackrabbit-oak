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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Convert a NodeState to a Map representation
     *
     * @param nodeState The NodeState to convert
     * @param maxDepth  Maximum depth to traverse
     * @return Map representation of the NodeState
     */
    public static Map<String, Object> convertNodeStateToMap(NodeState nodeState, int maxDepth, boolean shouldSerializeHiddenNodesOrProperties) {
        return convertNodeStateToMap(nodeState, maxDepth, -1, shouldSerializeHiddenNodesOrProperties);
    }

    /**
     * Convert a NodeState to a Map representation
     *
     * @param nodeState    The NodeState to convert
     * @param maxDepth     Maximum depth to traverse
     * @param currentDepth Current traversal depth
     * @return Map representation of the NodeState
     */
    private static Map<String, Object> convertNodeStateToMap(NodeState nodeState, int maxDepth, int currentDepth, boolean shouldSerializeHiddenNodesOrProperties) {
        if (maxDepth >= 0 && currentDepth >= maxDepth) {
            return null;
        }

        Map<String, Object> result = new HashMap<>();

        // Convert properties
        for (PropertyState property : nodeState.getProperties()) {
            String name = property.getName();
            Type<?> type = property.getType();
            // Skip serializing hidden properties.
            if (!shouldSerializeHiddenNodesOrProperties && name.startsWith(":")) {
                continue;
            }
            if (property.isArray()) {
                if (type == Type.STRINGS) {
                    result.put(name, property.getValue(Type.STRINGS));
                } else if (type == Type.LONGS) {
                    result.put(name, property.getValue(Type.LONGS));
                } else if (type == Type.DOUBLES) {
                    result.put(name, property.getValue(Type.DOUBLES));
                } else if (type == Type.BOOLEANS) {
                    result.put(name, property.getValue(Type.BOOLEANS));
                } else if (type == Type.DATES) {
                    result.put(name, property.getValue(Type.DATES));
                } else if (type == Type.DECIMALS) {
                    result.put(name, property.getValue(Type.DECIMALS));
                } else {
                    // For other array types, convert to strings
                    List<String> values = new ArrayList<>();
                    for (int i = 0; i < property.count(); i++) {
                        values.add(property.getValue(Type.STRING, i));
                    }
                    result.put(name, values);
                }
            } else {
                if (type == Type.STRING) {
                    result.put(name, property.getValue(Type.STRING));
                } else if (type == Type.LONG) {
                    result.put(name, property.getValue(Type.LONG));
                } else if (type == Type.DOUBLE) {
                    result.put(name, property.getValue(Type.DOUBLE));
                } else if (type == Type.BOOLEAN) {
                    result.put(name, property.getValue(Type.BOOLEAN));
                } else if (type == Type.DATE) {
                    result.put(name, property.getValue(Type.DATE));
                } else if (type == Type.DECIMAL) {
                    result.put(name, property.getValue(Type.DECIMAL));
                } else {
                    // For other types, convert to string
                    result.put(name, property.getValue(Type.STRING));
                }
            }
        }

        // Convert child nodes recursively
        for (String childName : nodeState.getChildNodeNames()) {
            if (!shouldSerializeHiddenNodesOrProperties && childName.startsWith(":")) {
                continue;
            }
            NodeState childNode = nodeState.getChildNode(childName);
            Map<String, Object> childMap = convertNodeStateToMap(childNode, maxDepth, currentDepth + 1, shouldSerializeHiddenNodesOrProperties);
            if (childMap != null) {
                result.put(childName, childMap);
            }
        }
        return result;
    }

    /**
     * Converts a NodeState to JSON string with specified depth
     *
     * @param nodeState The NodeState to convert
     * @param maxDepth  Maximum depth to traverse, use -1 for unlimited depth
     * @return JSON string representation
     * @throws JsonProcessingException if JSON processing fails
     */
    public static String nodeStateToJson(NodeState nodeState, int maxDepth) throws JsonProcessingException {
        JsonNode jsonNode = convertNodeStateToJson(nodeState, maxDepth, 0);
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
    }

    private static JsonNode convertNodeStateToJson(NodeState nodeState, int maxDepth, int currentDepth) {
        ObjectNode result = mapper.createObjectNode();

        // Return if max depth reached
        if (maxDepth != -1 && currentDepth > maxDepth) {
            return result;
        }

        // Convert properties
        for (PropertyState property : nodeState.getProperties()) {
            String name = property.getName();
            Type<?> type = property.getType();

            if (property.isArray()) {
                ArrayNode arrayNode = mapper.createArrayNode();
                if (type == Type.STRINGS) {
                    property.getValue(Type.STRINGS).forEach(arrayNode::add);
                } else if (type == Type.LONGS) {
                    property.getValue(Type.LONGS).forEach(arrayNode::add);
                } else if (type == Type.DOUBLES) {
                    property.getValue(Type.DOUBLES).forEach(arrayNode::add);
                } else if (type == Type.BOOLEANS) {
                    property.getValue(Type.BOOLEANS).forEach(arrayNode::add);
                } else if (type == Type.DATES) {
                    property.getValue(Type.DATES).forEach(arrayNode::add);
                } else if (type == Type.DECIMALS) {
                    property.getValue(Type.DECIMALS).forEach(arrayNode::add);
                } else {
                    // For other array types, convert to strings
                    for (int i = 0; i < property.count(); i++) {
                        arrayNode.add(property.getValue(Type.STRING, i));
                    }
                }
                result.set(name, arrayNode);
            } else {
                if (type == Type.STRING) {
                    result.put(name, property.getValue(Type.STRING));
                } else if (type == Type.LONG) {
                    result.put(name, property.getValue(Type.LONG));
                } else if (type == Type.DOUBLE) {
                    result.put(name, property.getValue(Type.DOUBLE));
                } else if (type == Type.BOOLEAN) {
                    result.put(name, property.getValue(Type.BOOLEAN));
                } else if (type == Type.DATE) {
                    result.put(name, property.getValue(Type.DATE).toString());
                } else if (type == Type.DECIMAL) {
                    result.put(name, property.getValue(Type.DECIMAL).toString());
                } else {
                    // For other types, convert to string
                    result.put(name, property.getValue(Type.STRING));
                }
            }
        }

        // Convert child nodes recursively
        for (String childName : nodeState.getChildNodeNames()) {
            NodeState childNode = nodeState.getChildNode(childName);
            result.set(childName, convertNodeStateToJson(childNode, maxDepth, currentDepth + 1));
        }

        return result;
    }

    public static boolean isValidJson(String text, boolean isJsonArray) {
        if (text == null) {
            return false;
        }

        JsopReader reader = new JsopTokenizer(text);
        return validateJson(reader, isJsonArray);
    }

    private static boolean validateJson(JsopReader reader, boolean isJsonArray) {
        if (reader.matches('{')) {
            return validateObject(reader) && reader.read() == JsopReader.END;
        }
        else if (reader.matches('[')) {
            if (!isJsonArray) {
                return false;
            }
            return validateArray(reader) && reader.read() == JsopReader.END;
        }
        else {
            return false;// readJsonValue(reader) && reader.read() == JsopReader.END;
        }
    }

    private static boolean validateObject(JsopReader reader) {
        boolean first = true;
        while (!reader.matches('}')) {
            if (!first && !reader.matches(',')) {
                return false;
            }
            if (!reader.matches(JsopReader.STRING)) {
                return false;
            }
            if (!reader.matches(':')) {
                return false;
            }
            if (!readJsonValue(reader)) {
                return false;
            }
            first = false;
        }
        return true;
    }

    private static boolean validateArray(JsopReader reader) {
        boolean first = true;
        while (!reader.matches(']')) {
            if (!first && !reader.matches(',')) {
                return false;
            }
            if (!readJsonValue(reader)) {
                return false;
            }
            first = false;
        }
        return true;
    }

    private static boolean readJsonValue(JsopReader reader) {
        int token = reader.read();
        switch (token) {
            case JsopReader.STRING:
            case JsopReader.NUMBER:
            case JsopReader.TRUE:
            case JsopReader.FALSE:
            case JsopReader.NULL:
                return true;
            case '{':
                return validateObject(reader);
            case '[':
                return validateArray(reader);
            default:
                return false;
        }
    }
}
