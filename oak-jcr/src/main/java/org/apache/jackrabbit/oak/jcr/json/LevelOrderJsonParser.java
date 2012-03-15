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
import java.util.Map;

/**
 * Utility class for parsing JSON objects and arrays into {@link JsonObject}s
 * and {@link JsonArray}s, respectively. In contrast to {@link FullJsonParser},
 * this implementation resolves nested structures lazily. That, is it does a
 * level order traverse of the JSON tree.
 * <p/>
 * The parser looks for 'hints' in the JSON text to speed up parsing: when it
 * encounters an integer value with the key ":size" in an object, that value
 * is used for the size of the entire object (including sub-objects).
 *
 * @see FullJsonParser
 */
public final class LevelOrderJsonParser {
    private LevelOrderJsonParser() { }
    
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
     * from its constituents. Nested objects are not fully parsed though, but a
     * reference to the parser is kept which is only invoked when that nested object
     * is actually accessed.
     */
    public static class ObjectHandler extends JsonHandler {
        private final JsonObject object = new JsonObject(new LinkedHashMap<String, JsonValue>());

        @Override
        public void atom(Token key, Token value) {
            object.put(key.text(), new JsonAtom(value));
        }

        @Override
        public void object(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            object.put(key.text(), new DeferredObjectValue(tokenizer.copy()));
            tokenizer.setPos(getNextPairPos(tokenizer.copy()));
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
     * from its constituents. Nested objects are not fully parsed though, but a
     * reference to the parser is kept which is only invoked when that nested object
     * is actually accessed.
     */
    public static class ArrayHandler extends JsonHandler {
        private final JsonArray array = new JsonArray(new ArrayList<JsonValue>());

        @Override
        public void atom(Token key, Token value) {
            array.add(new JsonAtom(value));
        }

        @Override
        public void object(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            array.add(new DeferredObjectValue(tokenizer.copy()));
            tokenizer.setPos(getNextPairPos(tokenizer.copy()));
        }

        @Override
        public void array(JsonParser parser, Token key, JsonTokenizer tokenizer) {
            array.add(parseArray(tokenizer));
        }

        public JsonArray getArray() {
            return array;
        }
    }

    //------------------------------------------< private >---

    private static class BreakException extends RuntimeException{
        private static final BreakException BREAK = new BreakException();
    }

    private static int getNextPairPos(JsonTokenizer tokenizer) {
        SkipObjectHandler skipObjectHandler = new SkipObjectHandler(tokenizer.pos());
        try {
            new JsonParser(skipObjectHandler).parseObject(tokenizer);
        }
        catch (BreakException e) {
            return skipObjectHandler.newPos;
        }
        return tokenizer.pos();
    }

    private static class DeferredObjectValue extends JsonObject {
        private final JsonTokenizer tokenizer;

        public DeferredObjectValue(JsonTokenizer tokenizer) {
            super(null);
            this.tokenizer = tokenizer;
        }

        @Override
        public void put(String key, JsonValue value) {
            throw new IllegalStateException("Cannot add value");
        }

        @Override
        public JsonValue get(String key) {
            return value().get(key);
        }

        @Override
        public Map<String, JsonValue> value() {
            return parseObject(tokenizer.copy()).value();
        }

        @Override
        public String toString() {
            return "<deferred>";
        }

    }

    private static class SkipObjectHandler extends JsonHandler {
        private final int startPos;
        private int newPos;

        public SkipObjectHandler(int startPos) {
            this.startPos = startPos;
        }

        @Override
        public void atom(Token key, Token value) {
            if (key != null && ":size".equals(key.text()) && Token.Type.NUMBER == value.type()) {
                newPos = startPos + Integer.parseInt(value.text());
                throw BreakException.BREAK;
            }
        }
    }
}
