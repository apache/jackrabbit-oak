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

package org.apache.jackrabbit.oak.jcr.json;

import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonArray;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonAtom;
import org.apache.jackrabbit.oak.jcr.json.JsonValue.JsonObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Utility class for parsing JSON objects and arrays into
 * {@link JsonObject}s and {@link JsonArray}s, respectively.
 *
 * @see LevelOrderJsonParser
 */
public class FullJsonParser {
    private FullJsonParser() { }

    /**
     * Parse a JSON object from {@code tokenizer}
     * @param tokenizer
     * @return a {@code JsonObject}
     * @throws ParseException
     */
    public static JsonObject parseObject(JsonTokenizer tokenizer) {
        ObjectHandler objectHandler = new ObjectHandler();
        new JsonParser(objectHandler).parseObject(tokenizer);
        return objectHandler.getObject();
    }

    /**
     * Parse a JSON array from {@code tokenizer}
     * @param tokenizer
     * @return a {@code JsonArray}
     * @throws ParseException
     */
    public static JsonArray parseArray(JsonTokenizer tokenizer) {
        ArrayHandler arrayHandler = new ArrayHandler();
        new JsonParser(arrayHandler).parseArray(tokenizer);
        return arrayHandler.getArray();
    }

    /**
     * This implementation of a {@code JsonHandler} builds up a {@code JsonObject}
     * by recursively descending into its constituents.  
     */
    public static class ObjectHandler extends JsonHandler {
        private final JsonObject object = new JsonObject(new LinkedHashMap<String, JsonValue>());

        @Override
        public void atom(Token key, Token value) {
            object.put(key.text(), new JsonAtom(value));
        }

        @Override
        public void object(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            object.put(key.text(), parseObject(tokenizer));
        }

        @Override
        public void array(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            object.put(key.text(), parseArray(tokenizer));
        }

        public JsonObject getObject() {
            return object;
        }

    }

    /**
     * This implementation of a {@code JsonHandler} builds up a {@code JsonArray}
     * by recursively descending into its constituents.
     */
    public static class ArrayHandler extends JsonHandler {
        private final JsonArray array = new JsonArray(new ArrayList<JsonValue>());

        @Override
        public void atom(Token key, Token value) {
            array.add(new JsonAtom(value));
        }

        @Override
        public void object(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            array.add(parseObject(tokenizer));
        }

        @Override
        public void array(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            array.add(parseArray(tokenizer));
        }

        public JsonArray getArray() {
            return array;
        }
    }

}
