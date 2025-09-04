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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.PropertyValue;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * A reader for tree store files.
 */
public class NodeTreeStoreReader implements NodeDataReader {

    private final TreeStore treeStore;
    private final Iterator<String> pathIterator;
    private final long fileSize;

    public static NodeDataReader open(String fileName) {
        BlobStore blobStore = null;
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        File file = new File(fileName);
        TreeStore treeStore = new TreeStore("reader", file, entryReader, 32);
        return new NodeTreeStoreReader(treeStore, file.length());
    }

    private NodeTreeStoreReader(TreeStore treeStore, long fileSize) {
        this.treeStore = treeStore;
        this.fileSize = fileSize;
        this.pathIterator = treeStore.iteratorOverPaths();
    }

    @Override
    public void close() throws IOException {
        treeStore.close();
    }

    @Override
    public NodeData readNode() throws IOException {
        if (!pathIterator.hasNext()) {
            return null;
        }
        String path = pathIterator.next();
        List<String> pathElements = new ArrayList<>();
        PathUtils.elements(path).forEach(pathElements::add);
        String nodeJson = treeStore.getSession().get(path);
        return new NodeData(pathElements, parse(nodeJson));
    }

    private static List<NodeProperty> parse(String nodeData) {
        ArrayList<NodeProperty> properties = new ArrayList<>();
        JsonObject json = JsonObject.fromJson(nodeData, true);
        for(Entry<String, String> e : json.getProperties().entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            NodeProperty p;
            if (v.startsWith("[")) {
                p = fromJsonArray(k, v);
            } else {
                PropertyValue value = getValue(v);
                if (value == null) {
                    // special case empty array
                    String v2 = JsopTokenizer.decodeQuoted(v);
                    if (v2.startsWith("[0]:")) {
                        String v3 = v2.substring(4);
                        int type = PropertyType.valueFromName(v3);
                        ValueType t = ValueType.byOrdinal(type);
                        p = new NodeProperty(k, t, new String[0], true);
                    } else {
                        throw new IllegalArgumentException(v);
                    }
                } else {
                    p = new NodeProperty(k, value.type, value.value);
                }
            }
            properties.add(p);
        }
        if (!json.getChildren().isEmpty()) {
            throw new IllegalArgumentException("Unexpected children " + json.getChildren());
        }
        return properties;
    }

    public static NodeProperty fromJsonArray(String key, String json) {
        ArrayList<String> result = new ArrayList<>();
        ValueType type = null;
        JsopTokenizer tokenizer = new JsopTokenizer(json);
        tokenizer.read('[');
        if (!tokenizer.matches(']')) {
            do {
                String r = tokenizer.readRawValue();
                PropertyValue v = getValue(r);
                if (v == null) {
                    throw new IllegalArgumentException("Array of empty arrays: " + json);
                } else if (type != null && v.type != type) {
                    throw new IllegalArgumentException("Unsupported mixed type: " + json);
                }
                result.add(v.value);
                type = v.type;
            } while (tokenizer.matches(','));
            tokenizer.read(']');
        }
        tokenizer.read(JsopReader.END);
        if (type == null) {
            type = ValueType.STRING;
        }
        return new NodeProperty(key, type, result.toArray(new String[result.size()]), true);
    }

    /**
     * Convert to a value if possible
     *
     * @param v the string
     * @return the property value, or null if the type is unknown (e.g. starts with "[0]:")
     */
    private static PropertyValue getValue(String v) {
        if (v.startsWith("\"")) {
            String v2 = JsopTokenizer.decodeQuoted(v);
            if (v2.length() > 3) {
                if (v2.startsWith(":blobId:")) {
                    return new PropertyValue(ValueType.BINARY, v2);
                } else if (v2.charAt(3) == ':') {
                    String v3 = v2.substring(4);
                    if (v2.startsWith("str:")) {
                        return new PropertyValue(ValueType.STRING, v3);
                    } else if (v2.startsWith("nam:")) {
                        return new PropertyValue(ValueType.NAME, v3);
                    } else if (v2.startsWith("ref:")) {
                        return new PropertyValue(ValueType.REFERENCE, v3);
                    } else if (v2.startsWith("dat:")) {
                        return new PropertyValue(ValueType.DATE, v3);
                    } else if (v2.startsWith("dec:")) {
                        return new PropertyValue(ValueType.DECIMAL, v3);
                    } else if (v2.startsWith("dou:")) {
                        return new PropertyValue(ValueType.DOUBLE, v3);
                    } else if (v2.startsWith("wea:")) {
                        return new PropertyValue(ValueType.WEAKREFERENCE, v3);
                    } else if (v2.startsWith("uri:")) {
                        return new PropertyValue(ValueType.URI, v3);
                    } else if (v2.startsWith("pat:")) {
                        return new PropertyValue(ValueType.PATH, v3);
                    } else {
                        // could be "[0]:"
                        return null;
                    }
                } else {
                    return new PropertyValue(ValueType.STRING, v2);
                }
            } else {
                return new PropertyValue(ValueType.STRING, v2);
            }
        } else if ("null".equals(v)) {
            return new PropertyValue(ValueType.NULL, null);
        } else if ("true".equals(v)) {
            return new PropertyValue(ValueType.BOOLEAN, v);
        } else if ("false".equals(v)) {
            return new PropertyValue(ValueType.BOOLEAN, v);
        } else {
            return new PropertyValue(ValueType.LONG, v);
        }
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public int getProgressPercent() {
        return 0;
    }

}
