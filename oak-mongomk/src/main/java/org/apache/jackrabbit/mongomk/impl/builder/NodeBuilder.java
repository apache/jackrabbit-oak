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
package org.apache.jackrabbit.mongomk.impl.builder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.json.JsonUtil;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A builder to create {@link Node}s from <a hred="http://en.wikipedia.org/wiki/JavaScript_Object_Notation">JSON</a>
 * strings.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class NodeBuilder {

    /**
     * Creates {@link Node} from the given {@code json} and an empty path as root path.
     *
     * @param json The {@code json}.
     * @return The {@code Node}.
     * @throws Exception If an error occurred while creating.
     * @see #build(String, String)
     */
    public static Node build(String json) throws Exception {
        return build(json, "");
    }

    /**
     * Creates {@link Node} from the given {@code json} and an empty path as root path.
     *
     * @param json The {@code json}.
     * @param path The root path of the nodes.
     * @return The {@code Node}.
     * @throws Exception If an error occurred while creating.
     * @see #build(String, String)
     */
    public static Node build(String json, String path) throws Exception {
        NodeBuilder nodeBuilder = new NodeBuilder();

        return nodeBuilder.doBuild(json, path);
    }

    private NodeBuilder() {
        // only private construction
    }

    private Node doBuild(String json, String path) throws Exception {
        try {
            JSONObject jsonObject = new JSONObject(json);
            JSONArray names = jsonObject.names();
            if (names.length() != 1) {
                throw new IllegalArgumentException("JSON must contain exactly 1 root node");
            }

            String name = names.getString(0);
            JSONObject value = jsonObject.getJSONObject(name);

            return parseNode(PathUtils.concat(path, name), value);
        } catch (JSONException e) {
            throw new Exception(e);
        }
    }

    private Node parseNode(String path, JSONObject jsonObject) throws Exception {
        String realPath = path;
        String revisionId = null;

        int index = path.lastIndexOf('#');
        if (index != -1) {
            realPath = path.substring(0, index);
            revisionId = path.substring(index + 1);
        }

        NodeImpl node = new NodeImpl(realPath);
        node.setRevisionId(MongoUtil.toMongoRepresentation(revisionId));

        Map<String, Object> properties = null;
        for (@SuppressWarnings("rawtypes")
        Iterator iterator = jsonObject.keys(); iterator.hasNext();) {
            String key = (String) iterator.next();
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {
                String childPath = PathUtils.concat(realPath, key);

                Node childNode = parseNode(childPath, (JSONObject) value);
                node.addChild(childNode);
            } else {
                if (properties == null) {
                    properties = new HashMap<String, Object>();
                }

                Object converted = JsonUtil.convertJsonValue(value.toString());
                properties.put(key, converted);
            }
        }

        node.setProperties(properties);

        return node;
    }
}
