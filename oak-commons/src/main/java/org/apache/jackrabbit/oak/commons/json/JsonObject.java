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
import java.util.Map;

/**
 * Simple JSON Object representation
 */
public class JsonObject {

    private Map<String, String> props = new HashMap<String, String>();
    private Map<String, JsonObject> children = new HashMap<String, JsonObject>();

    /**
     * Reads a JSON object from the given tokenizer. The opening '{' of the
     * object should already have been consumed from the tokenizer before
     * this method is called.
     *
     * @param t tokenizer
     * @return JSON object
     */
    public static JsonObject create(JsopTokenizer t) {
        JsonObject obj = new JsonObject();
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    obj.children.put(key, create(t));
                } else {
                    obj.props.put(key, t.readRawValue().trim());
                }
            } while (t.matches(','));
            t.read('}');
        }
        return obj;
    }

    public void toJson(JsopBuilder buf) {
        toJson(buf, this);
    }

    public Map<String, String> getProperties() {
        return props;
    }

    public Map<String, JsonObject> getChildren() {
        return children;
    }

    private static void toJson(JsopBuilder buf, JsonObject obj) {
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