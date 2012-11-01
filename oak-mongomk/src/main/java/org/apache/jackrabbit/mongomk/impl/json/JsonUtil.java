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

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.ChildNode;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.PropertyState;
import org.apache.jackrabbit.mk.util.NameFilter;
import org.apache.jackrabbit.mk.util.NodeFilter;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * JSON related utility class.
 */
public class JsonUtil {

    public static Object toJsonValue(String jsonValue) throws Exception {
        if (jsonValue == null) {
            return null;
        }

        JSONObject jsonObject = new JSONObject("{dummy : " + jsonValue + "}");
        Object obj = jsonObject.get("dummy");
        return convertJsonValue(obj);
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

    // Most of this method borrowed from MicroKernelImpl#toJson. It'd be nice if
    // this somehow consolidated with MicroKernelImpl#toJson.
    public static void toJson(JsopBuilder builder, NodeState node, int depth,
            int offset, int maxChildNodes, boolean inclVirtualProps, NodeFilter filter) {

        for (PropertyState property : node.getProperties()) {
            if (filter == null || filter.includeProperty(property.getName())) {
                builder.key(property.getName()).encodedValue(property.getEncodedValue());
            }
        }

        long childCount = node.getChildNodeCount();
        if (inclVirtualProps) {
            if (filter == null || filter.includeProperty(":childNodeCount")) {
                // :childNodeCount is by default always included
                // unless it is explicitly excluded in the filter
                builder.key(":childNodeCount").value(childCount);
            }
        }

        if (childCount <= 0 || depth < 0) {
            return;
        }

        if (filter != null) {
            NameFilter childFilter = filter.getChildNodeFilter();
            if (childFilter != null && !childFilter.containsWildcard()) {
                // Optimization for large child node lists:
                // no need to iterate over the entire child node list if the filter
                // does not include wildcards
                int count = maxChildNodes == -1 ? Integer.MAX_VALUE : maxChildNodes;
                for (String name : childFilter.getInclusionPatterns()) {
                    NodeState child = node.getChildNode(name);
                    if (child != null) {
                        boolean incl = true;
                        for (String exclName : childFilter.getExclusionPatterns()) {
                            if (name.equals(exclName)) {
                                incl = false;
                                break;
                            }
                        }
                        if (incl) {
                            if (count-- <= 0) {
                                break;
                            }
                            builder.key(name).object();
                            if (depth > 0) {
                                toJson(builder, child, depth - 1, 0, maxChildNodes, inclVirtualProps, filter);
                            }
                            builder.endObject();
                        }
                    }
                }
                return;
            }
        }

        int count = maxChildNodes;
        if (count != -1 && filter != null && filter.getChildNodeFilter() != null) {
            // Specific maxChildNodes limit and child node filter
            count = -1;
        }
        int numSiblings = 0;
        for (ChildNode entry : node.getChildNodeEntries(offset, count)) {

            if (filter == null || filter.includeNode(entry.getName())) {
                if (maxChildNodes != -1 && ++numSiblings > maxChildNodes) {
                    break;
                }
                builder.key(entry.getName()).object();
                if (depth > 0) {
                    toJson(builder, entry.getNode(), depth - 1, 0, maxChildNodes, inclVirtualProps, filter);
                }
                builder.endObject();
            }
        }
    }
}