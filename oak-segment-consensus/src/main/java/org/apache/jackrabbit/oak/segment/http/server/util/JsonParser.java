/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.http.server.util;

import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple JSON parsing utilities.
 * 
 * <p>Extracted from SegmentHttpServer for better organization.
 * Note: This is a simple parser for Phase 1. Consider using Jackson or Gson in the future.</p>
 */
public class JsonParser {
    
    /**
     * Extract a JSON field value from a JSON string.
     * 
     * @param json The JSON string
     * @param field The field name to extract
     * @return The field value, or null if not found
     */
    public static String extractField(String json, String field) {
        int start = json.indexOf("\"" + field + "\"");
        if (start == -1) return null;
        
        start = json.indexOf(":", start) + 1;
        
        // Skip whitespace
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        
        // Check if value is quoted (string) or unquoted (number/boolean/null)
        if (start >= json.length()) return null;
        
        if (json.charAt(start) == '"') {
            JsopTokenizer tokenizer = new JsopTokenizer(json, start);
            return tokenizer.matches(JsopReader.STRING) ? tokenizer.getToken() : null;
        } else {
            // Unquoted value (number, boolean, or null) - read until comma, }, or ]
            int end = start;
            while (end < json.length()) {
                char c = json.charAt(end);
                if (c == ',' || c == '}' || c == ']' || Character.isWhitespace(c)) {
                    break;
                }
                end++;
            }
            return json.substring(start, end).trim();
        }
    }
    
    /**
     * Extract a JSON object (not a primitive) from a JSON string.
     * Used to extract nested objects like the proof and handover.
     * 
     * @param json The JSON string
     * @param field The field name containing the object
     * @return The JSON object as a string, or null if not found
     */
    public static String extractObject(String json, String field) {
        String pattern = "\"" + field + "\":";
        int start = json.indexOf(pattern);
        if (start == -1) return null;
        
        start = json.indexOf("{", start);
        if (start == -1) return null;
        
        // Find matching closing brace
        int depth = 0;
        int end = start;
        while (end < json.length()) {
            char c = json.charAt(end);
            if (c == '{') depth++;
            if (c == '}') {
                depth--;
                if (depth == 0) {
                    return json.substring(start, end + 1);
                }
            }
            end++;
        }
        
        return null;
    }

    /**
     * Parse a JSON object with full string escape handling. Values are mapped to
     * {@code String}, {@code Long} (integral numbers), {@code BigDecimal} (other numbers),
     * {@code Boolean}, {@code null}, nested {@code Map} and {@code List}. For duplicate
     * keys the first occurrence wins.
     *
     * @throws IllegalArgumentException if the input is not a single JSON object
     */
    public static Map<String, Object> parseObject(String json) {
        JsopTokenizer tokenizer = new JsopTokenizer(json);
        tokenizer.read('{');
        Map<String, Object> result = readObject(tokenizer);
        tokenizer.read(JsopReader.END);
        return result;
    }

    private static Map<String, Object> readObject(JsopTokenizer t) {
        Map<String, Object> object = new LinkedHashMap<>();
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                Object value = readValue(t);
                if (!object.containsKey(key)) {
                    object.put(key, value);
                }
            } while (t.matches(','));
            t.read('}');
        }
        return object;
    }

    private static Object readValue(JsopTokenizer t) {
        if (t.matches('{')) {
            return readObject(t);
        }
        if (t.matches('[')) {
            List<Object> list = new ArrayList<>();
            if (!t.matches(']')) {
                do {
                    list.add(readValue(t));
                } while (t.matches(','));
                t.read(']');
            }
            return list;
        }
        if (t.matches(JsopReader.STRING)) {
            return t.getToken();
        }
        if (t.matches(JsopReader.NUMBER)) {
            String number = t.getToken();
            try {
                return Long.parseLong(number);
            } catch (NumberFormatException e) {
                return new BigDecimal(number);
            }
        }
        if (t.matches(JsopReader.TRUE)) {
            return Boolean.TRUE;
        }
        if (t.matches(JsopReader.FALSE)) {
            return Boolean.FALSE;
        }
        t.read(JsopReader.NULL);
        return null;
    }
}
