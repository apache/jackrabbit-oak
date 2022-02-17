/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons.json;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple JSON Object representation.
 * 
 * It optionally supports respecting the order of properties, and the order children.
 */
public class JsonObject {

    private Map<String, String> props;
    private Map<String, JsonObject> children;

    /**
     * Create a Json object that doesn't respect the order.
     */
    public JsonObject() {
        this(false);
    }
    
    /**
     * Create a Json object.
     * 
     * @param respectOrder whether the object should respect the order
     */
    public JsonObject(boolean respectOrder) {
        props = map(respectOrder);
        children = map(respectOrder);
    }
    
    private static <K, V> Map<K, V> map(boolean respectOrder) {
        return respectOrder ? new LinkedHashMap<K, V>() : new HashMap<K, V>();
    }
    
    /**
     * Build a Json object from a String.
     * 
     * @param json the json string
     * @param respectOrder whether the object should respect the child order
     * @return the json object
     */
    public static JsonObject fromJson(String json, boolean respectOrder) {
        JsopTokenizer tokenizer = new JsopTokenizer(json);
        tokenizer.read('{');
        try {
            return create(tokenizer, respectOrder);
        } finally {
            tokenizer.read(JsopReader.END);
        }
    }
    
    /**
     * Reads a JSON object from the given tokenizer. The opening '{' of the
     * object should already have been consumed from the tokenizer before
     * this method is called.
     *
     * @param t tokenizer
     * @param respectOrder whether the order should be respected
     * @return JSON object
     */
    public static JsonObject create(JsopTokenizer t, boolean respectOrder) {
        JsonObject obj = new JsonObject(respectOrder);
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    obj.children.put(key, create(t, respectOrder));
                } else {
                    obj.props.put(key, t.readRawValue().trim());
                }
            } while (t.matches(','));
            t.read('}');
        }
        return obj;        
    }

    /**
     * Reads a JSON object from the given tokenizer. The opening '{' of the
     * object should already have been consumed from the tokenizer before
     * this method is called.
     *
     * @param t tokenizer
     * @return JSON object
     */
    public static JsonObject create(JsopTokenizer t) {
        return create(t, false);
    }

    /**
     * Write the object to a builder.
     * 
     * @param buf the target
     */
    public void toJson(JsopBuilder buf) {
        toJson(buf, this);
    }

    /**
     * Get the (mutable) map of properties.
     * 
     * @return the property map
     */
    public Map<String, String> getProperties() {
        return props;
    }

    /**
     * Get the (mutable) map of children.
     * 
     * @return the children map
     */
    public Map<String, JsonObject> getChildren() {
        return children;
    }

    /**
     * Pretty-print the object.
     * 
     * @return the pretty-printed string representation
     */
    @Override
    public String toString() {
        JsopBuilder w = new JsopBuilder();
        toJson(w);
        return JsopBuilder.prettyPrint(w.toString());
    }

    private static void toJson(JsopBuilder buf, JsonObject obj) {
        if (obj == null) {
            buf.value(null);
        } else {
            buf.object();
            for (String name : obj.props.keySet()) {
                buf.key(name).encodedValue(obj.props.get(name));
            }
            for (String name : obj.children.keySet()) {
                buf.key(name);
                toJson(buf, obj.children.get(name));
            }
            buf.endObject();
        }
    }

}