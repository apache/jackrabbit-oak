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
package org.apache.jackrabbit.mongomk.impl.json;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.util.NodeFilter;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * FIXME - [Mete] This should really merge with MicroKernelImpl#toJson.
 *
 * <a href="http://en.wikipedia.org/wiki/JavaScript_Object_Notation">JSON</a> related utility classes.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class JsonUtil {

    public static Object convertJsonValue(String jsonValue) throws Exception {
        if (jsonValue == null) {
            return null;
        }

        String dummyJson = "{dummy : " + jsonValue + "}";
        JSONObject jsonObject = new JSONObject(dummyJson);
        Object dummyObject = jsonObject.get("dummy");
        return convertJsonValue(dummyObject);
    }

    public static String convertToJson(Node node, int depth, int offset, int maxChildNodes,
            boolean inclVirtualProps, NodeFilter filter) {
        JsopBuilder builder = new JsopBuilder();
        convertToJson(builder, node, depth, 0, offset, maxChildNodes, inclVirtualProps, filter);
        return builder.toString();
    }

    static void convertToJson(JsopBuilder builder, Node node, int depth, int currentDepth,
            int offset, int maxChildNodes, boolean inclVirtualProps,  NodeFilter filter) {
        builder.object();

        Map<String, Object> properties = node.getProperties();
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String key = entry.getKey();
                if (filter == null || filter.includeProperty(key)) {
                    Object value = entry.getValue();
                    builder.key(key);
                    if (value instanceof String) {
                        builder.value(value.toString());
                    } else {
                        builder.encodedValue(value.toString());
                    }
                }
            }
        }

        long childCount = node.getChildCount();
        if (inclVirtualProps) {
            if (filter == null || filter.includeProperty(":childNodeCount")) {
                // :childNodeCount is by default always included
                // unless it is explicitly excluded in the filter
                builder.key(":childNodeCount").value(childCount);
            }
            // FIXME [Mete] See if :hash is still being used.
            /*check whether :hash has been explicitly included
            if (filter != null) {
                NameFilter nf = filter.getPropertyFilter();
                if (nf != null
                        && nf.getInclusionPatterns().contains(":hash")
                        && !nf.getExclusionPatterns().contains(":hash")) {
                    builder.key(":hash").value(rep.getRevisionStore().getId(node).toString());
                }
            }
            */
        }

        // FIXME [Mete] There's still some more work here.
        Iterator<Node> entries = node.getChildEntries(offset, maxChildNodes);
        while (entries.hasNext()) {
            Node child = entries.next();
            int numSiblings = 0;
            if (maxChildNodes != -1 && ++numSiblings > maxChildNodes) {
                break;
            }
            builder.key(child.getName());
            if ((depth == -1) || (currentDepth < depth)) {
                convertToJson(builder, child, depth, currentDepth + 1, offset,
                        maxChildNodes, inclVirtualProps, filter);
            } else {
                builder.object();
                builder.endObject();
            }
        }

        builder.endObject();
    }

    private static Object convertJsonValue(Object jsonObject) throws Exception {
        if (jsonObject == JSONObject.NULL) {
            return null;
        }

        if (jsonObject instanceof JSONArray) {
            List<Object> elements = new LinkedList<Object>();
            JSONArray dummyArray = (JSONArray) jsonObject;
            for (int i = 0; i < dummyArray.length(); ++i) {
                Object raw = dummyArray.get(i);
                Object parsed = convertJsonValue(raw);
                elements.add(parsed);
            }
            return elements;
        }

        return jsonObject;
    }

    private JsonUtil() {
        // no instantiation
    }
}
