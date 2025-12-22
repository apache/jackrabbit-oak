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
package org.apache.jackrabbit.oak.plugins.index.diff;

import java.util.Map.Entry;
import java.util.TreeSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to persist a configuration that is in the form of JSON into
 * the node store.
 *
 * This is used to persist a small set of configuration nodes, eg. index
 * definitions, using a simple JSON format.
 *
 * The node type does not need to be set on a per-node basis. Where it is
 * missing, the provided node type is used (e.g. "nt:unstructured")
 *
 * A "jcr:uuid" is automatically added for nodes of type "nt:resource".
 *
 * String, string arrays, boolean, blob, long, and double values are supported.
 * Values that start with ":blobId:...base64..." are stored as binaries. "str:",
 * "nam:" and "dat:" prefixes are removed.
 *
 * "null" entries are not supported.
 */
public class JsonNodeBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(JsonNodeBuilder.class);

    /**
     * Add a replace a node, including all child nodes, in the node store.
     *
     * @param nodeStore the target node store
     * @param targetPath the target path where the node(s) is/are replaced
     * @param nodeType the node type of the new node (eg. "nt:unstructured")
     * @param jsonString the json string with the node data
     * @throws CommitFailedException if storing the nodes failed
     * @throws IOException if storing a blob failed
     */
    public static void addOrReplace(NodeBuilder builder, NodeStore nodeStore, String targetPath, String nodeType, String jsonString) throws CommitFailedException, IOException {
        LOG.debug("Storing {}: {}", targetPath, jsonString);
        if (nodeType.indexOf("/") >= 0) {
            throw new IllegalStateException("Illegal node type: " + nodeType);
        }
        JsonObject json = JsonObject.fromJson(jsonString, true);
        for (String name : PathUtils.elements(targetPath)) {
            NodeBuilder child = builder.child(name);
            if (!child.hasProperty("jcr:primaryType")) {
                child.setProperty("jcr:primaryType", nodeType, Type.NAME);
            }
            builder = child;
        }
        storeConfigNode(nodeStore, builder, nodeType, json);
    }

    private static void storeConfigNode(NodeStore nodeStore, NodeBuilder builder, String nodeType, JsonObject json) throws IOException {
        ArrayList<String> childOrder = new ArrayList<>();
        for (Entry<String, JsonObject> e : json.getChildren().entrySet()) {
            String k = e.getKey();
            childOrder.add(k);
            JsonObject v = e.getValue();
            storeConfigNode(nodeStore, builder.child(k), nodeType, v);
        }
        for (String child : builder.getChildNodeNames()) {
            if (!json.getChildren().containsKey(child)) {
                builder.child(child).remove();
            }
        }
        for (Entry<String, String> e : json.getProperties().entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            storeConfigProperty(nodeStore, builder, k, v);
        }
        if (!json.getProperties().containsKey("jcr:primaryType")) {
            builder.setProperty("jcr:primaryType", nodeType, Type.NAME);
        }
        for (PropertyState prop : builder.getProperties()) {
            if ("jcr:primaryType".equals(prop.getName())) {
                continue;
            }
            if (!json.getProperties().containsKey(prop.getName())) {
                builder.removeProperty(prop.getName());
            }
        }
        builder.setProperty(TreeConstants.OAK_CHILD_ORDER, childOrder, Type.NAMES);
        if ("nt:resource".equals(JsonNodeBuilder.oakStringValue(json, "jcr:primaryType"))) {
            if (!json.getProperties().containsKey("jcr:uuid")) {
                String uuid = UUID.randomUUID().toString();
                builder.setProperty("jcr:uuid", uuid);
            }
        }
    }

    private static void storeConfigProperty(NodeStore nodeStore, NodeBuilder builder, String propertyName, String value) throws IOException {
        if (value.startsWith("\"")) {
            // string or blob
            value = JsopTokenizer.decodeQuoted(value);
            if (value.startsWith(":blobId:")) {
                String base64 = value.substring(":blobId:".length());
                byte[] bytes = Base64.getDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
                if (nodeStore == null) {
                    MemoryNodeStore mns = new MemoryNodeStore();
                    Blob blob = mns.createBlob(new ByteArrayInputStream(bytes));
                    builder.setProperty(propertyName, blob);
                } else {
                    Blob blob = nodeStore.createBlob(new ByteArrayInputStream(bytes));
                    builder.setProperty(propertyName, blob);
                }
            } else {
                if (value.startsWith("str:") || value.startsWith("nam:") || value.startsWith("dat:")) {
                    value = value.substring("str:".length());
                }
                if ("jcr:primaryType".equals(propertyName)) {
                    builder.setProperty(propertyName, value, Type.NAME);
                } else {
                    builder.setProperty(propertyName, value);
                }
            }
        } else if ("null".equals(value)) {
            throw new IllegalArgumentException("Removing entries is not supported for property " + propertyName);
        } else if ("true".equals(value)) {
            builder.setProperty(propertyName, true);
        } else if ("false".equals(value)) {
            builder.setProperty(propertyName, false);
        } else if (value.startsWith("[")) {
            JsopTokenizer tokenizer = new JsopTokenizer(value);
            ArrayList<String> result = new ArrayList<>();
            tokenizer.matches('[');
            if (!tokenizer.matches(']')) {
                do {
                    if (!tokenizer.matches(JsopReader.STRING)) {
                        throw new IllegalArgumentException("Could not process string array " + value + " for property " + propertyName);
                    }
                    result.add(tokenizer.getEscapedToken());
                } while (tokenizer.matches(','));
                tokenizer.read(']');
            }
            tokenizer.read(JsopReader.END);
            builder.setProperty(propertyName, result, Type.STRINGS);
        } else if (value.indexOf('.') >= 0 || value.toLowerCase().indexOf("e") >= 0) {
            // double
            try {
                Double d = Double.parseDouble(value);
                builder.setProperty(propertyName, d);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse double " + value + " for property " + propertyName);
            }
        } else if (value.startsWith("-") || (!value.isEmpty() && Character.isDigit(value.charAt(0)))) {
            // long
            try {
                Long x = Long.parseLong(value);
                builder.setProperty(propertyName, x);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse long " + value + " for property " + propertyName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported value " + value + " for property " + propertyName);
        }
    }

    public static String oakStringValue(JsonObject json, String propertyName) {
        String value = json.getProperties().get(propertyName);
        if (value == null) {
            return null;
        }
        return oakStringValue(value);
    }

    static String oakStringValue(String value) {
        if (!value.startsWith("\"")) {
            // support numbers
            return value;
        }
        value = JsopTokenizer.decodeQuoted(value);
        if (value.startsWith(":blobId:")) {
            value = value.substring(":blobId:".length());
            value = new String(Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        } else if (value.startsWith("str:") || value.startsWith("nam:") || value.startsWith("dat:")) {
            value = value.substring("str:".length());
        }
        return value;
    }

    /**
     * Read an Oak string array. There are 3 cases:
     *
     * - the property doesn't exist: return null
     * - the value is stored as string: return an array with one entry
     * - the value is stored in an array: return the sorted list of value
     *
     * The value is sorted, because the order is insignificant in our case,
     * and want ["a", "b"] = ["b", "a"] when comparing index definitions.
     *
     * @param json the JSON object
     * @param propertyName the property to extract
     * @return a string array or null
     */
    public static String[] oakStringArrayValue(JsonObject json, String propertyName) {
        String value = json.getProperties().get(propertyName);
        if (value == null) {
            return null;
        } else if (value.startsWith("\"")) {
            return new String[] { oakStringValue(value) };
        } else if (value.startsWith("[")) {
            return JsonNodeBuilder.getStringSet(value).toArray(new String[0]);
        } else {
            LOG.warn("Unsupported value type: {}", value);
            return null;
        }
    }

    public static TreeSet<String> getStringSet(String value) {
        if (value == null) {
            return null;
        }
        try {
            JsopTokenizer tokenizer = new JsopTokenizer(value);
            TreeSet<String> result = new TreeSet<>();
            if (tokenizer.matches(JsopReader.STRING)) {
                result.add(tokenizer.getEscapedToken());
                return result;
            }
            if (!tokenizer.matches('[')) {
                return null;
            }
            if (!tokenizer.matches(']')) {
                do {
                    if (!tokenizer.matches(JsopReader.STRING)) {
                        // not a string
                        return null;
                    }
                    result.add(tokenizer.getEscapedToken());
                } while (tokenizer.matches(','));
                tokenizer.read(']');
            }
            tokenizer.read(JsopReader.END);
            return result;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
