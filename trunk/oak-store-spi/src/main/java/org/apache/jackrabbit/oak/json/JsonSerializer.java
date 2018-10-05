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
package org.apache.jackrabbit.oak.json;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.util.List;
import java.util.regex.Pattern;

import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Utility class for serializing node and property states to JSON.
 */
public class JsonSerializer {
    public static final String DEFAULT_FILTER_EXPRESSION =
        "{\"properties\":[\"*\", \"-:childNodeCount\"]}";

    private static final JsonFilter DEFAULT_FILTER = new JsonFilter(DEFAULT_FILTER_EXPRESSION);

    private final JsopWriter json;

    private final int depth;

    private final long offset;

    private final int maxChildNodes;

    private final JsonFilter filter;

    private final BlobSerializer blobs;

    private JsonSerializer(
            JsopWriter json, int depth, long offset, int maxChildNodes,
            JsonFilter filter, BlobSerializer blobs) {
        this.json = checkNotNull(json);
        this.depth = depth;
        this.offset = offset;
        this.maxChildNodes = maxChildNodes;
        this.filter = checkNotNull(filter);
        this.blobs = checkNotNull(blobs);
    }

    public JsonSerializer(
            int depth, long offset, int maxChildNodes,
            String filter, BlobSerializer blobs) {
        this(new JsopBuilder(), depth, offset, maxChildNodes,
                new JsonFilter(filter), blobs);
    }

    public JsonSerializer(JsopWriter json,
            int depth, long offset, int maxChildNodes,
            String filter, BlobSerializer blobs) {
        this(json, depth, offset, maxChildNodes,
                new JsonFilter(filter), blobs);
    }

    public JsonSerializer(JsopWriter json, BlobSerializer blobs) {
        this(json, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                DEFAULT_FILTER, blobs);
    }

    public JsonSerializer(JsopWriter json, String filter, BlobSerializer blobs) {
        this(json, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                new JsonFilter(filter), blobs);
    }

    protected JsonSerializer getChildSerializer() {
        return new JsonSerializer(
                json, depth - 1, 0, maxChildNodes, filter, blobs);
    }

    public void serialize(NodeState node) {
        json.object();

        for (PropertyState property : node.getProperties()) {
            String name = property.getName();
            if (filter.includeProperty(name)) {
                json.key(name);
                serialize(property);
            }
        }

        int index = 0;
        int count = 0;
        for (ChildNodeEntry child : getChildNodeEntries(node)) {
            String name = child.getName();
            if (filter.includeNode(name) && index++ >= offset) {
                if (count++ >= maxChildNodes) {
                    break;
                }

                json.key(name);
                if (depth > 0) {
                    getChildSerializer().serialize(child.getNodeState());
                } else {
                    json.object();
                    json.endObject();
                }
            }
        }

        json.endObject();
    }

    private Iterable<? extends ChildNodeEntry> getChildNodeEntries(NodeState node) {
        PropertyState order = node.getProperty(":childOrder");
        if (order != null) {
            List<String> names = ImmutableList.copyOf(order.getValue(NAMES));
            List<ChildNodeEntry> entries = Lists.newArrayListWithCapacity(names.size());
            for (String name : names) {
                entries.add(new MemoryChildNodeEntry(name, node.getChildNode(name)));
            }
            return entries;
        }
        return node.getChildNodeEntries();
    }

    public void serialize(PropertyState property) {
        Type<?> type = property.getType();
        if (!type.isArray()) {
            serialize(property, type, 0);
        } else {
            Type<?> base = type.getBaseType();
            int count = property.count();
            if (base == STRING || count > 0) {
                json.array();
                for (int i = 0; i < count; i++) {
                    serialize(property, base, i);
                }
                json.endArray();
            } else {
                // type-safe encoding of an empty array
                json.value(TypeCodes.EMPTY_ARRAY
                        + PropertyType.nameFromValue(type.tag()));
            }
        }
    }

    public void serialize(PropertyState property, Type<?> type, int index) {
        if (type == BOOLEAN) {
            json.value(property.getValue(BOOLEAN, index).booleanValue());
        } else if (type == LONG) {
            json.value(property.getValue(LONG, index).longValue());
        } else if (type == DOUBLE) {
            Double value = property.getValue(DOUBLE, index);
            if (value.isNaN() || value.isInfinite()) {
                json.value(TypeCodes.encode(type.tag(), value.toString()));
            } else {
                json.encodedValue(value.toString());
            }
        } else if (type == BINARY) {
            Blob blob = property.getValue(BINARY, index);
            json.value(TypeCodes.encode(type.tag(), blobs.serialize(blob)));
        } else  {
            String value = property.getValue(STRING, index);
            if (type != STRING || TypeCodes.split(value) != -1) {
                value = TypeCodes.encode(type.tag(), value);
            }
            json.value(value);
        }
    }

    public String toString() {
        return json.toString();
    }

    /**
     * Utility class for deciding which nodes and properties to serialize.
     */
    private static class JsonFilter {

        private static final Pattern EVERYTHING = Pattern.compile(".*");

        private final List<Pattern> nodeIncludes = newArrayList(EVERYTHING);

        private final List<Pattern> nodeExcludes = newArrayList();

        private final List<Pattern> propertyIncludes = newArrayList(EVERYTHING);

        private final List<Pattern> propertyExcludes = newArrayList();

        JsonFilter(String filter) {
            JsopTokenizer tokenizer = new JsopTokenizer(filter);
            tokenizer.read('{');
            for (boolean first = true; !tokenizer.matches('}'); first = false) {
                if (!first) {
                    tokenizer.read(',');
                }
                String key = tokenizer.readString();
                tokenizer.read(':');

                List<Pattern> includes = newArrayList();
                List<Pattern> excludes = newArrayList();
                readPatterns(tokenizer, includes, excludes);

                if (key.equals("nodes")) {
                    nodeIncludes.clear();
                    nodeIncludes.addAll(includes);
                    nodeExcludes.clear();
                    nodeExcludes.addAll(excludes);
                } else if (key.equals("properties")) {
                    propertyIncludes.clear();
                    propertyIncludes.addAll(includes);
                    propertyExcludes.clear();
                    propertyExcludes.addAll(excludes);
                } else {
                    throw new IllegalStateException(key);
                }
            }
        }

        private static void readPatterns(JsopTokenizer tokenizer, List<Pattern> includes,
                List<Pattern> excludes) {
            tokenizer.read('[');
            for (boolean first = true; !tokenizer.matches(']'); first = false) {
                if (!first) {
                    tokenizer.read(',');
                }
                String pattern = tokenizer.readString();
                if (pattern.startsWith("-")) {
                    excludes.add(glob(pattern.substring(1)));
                } else if (pattern.startsWith("\\-")) {
                    includes.add(glob(pattern.substring(1)));
                } else {
                    includes.add(glob(pattern));
                }
            }
        }

        private static Pattern glob(String pattern) {
            StringBuilder builder = new StringBuilder();
            int star = pattern.indexOf('*');
            while (star != -1) {
                if (star > 0 && pattern.charAt(star - 1) == '\\') {
                    builder.append(Pattern.quote(pattern.substring(0, star - 1)));
                    builder.append(Pattern.quote("*"));
                } else {
                    builder.append(Pattern.quote(pattern.substring(0, star)));
                    builder.append(".*");
                }
                pattern = pattern.substring(star + 1);
                star = pattern.indexOf('*');
            }
            builder.append(Pattern.quote(pattern));
            return Pattern.compile(builder.toString());
        }

        boolean includeNode(String name) {
            return include(name, nodeIncludes, nodeExcludes);
        }

        boolean includeProperty(String name) {
            return include(name, propertyIncludes, propertyExcludes);
        }

        private static boolean include(
                String name, List<Pattern> includes, List<Pattern> excludes) {
            for (Pattern include : includes) {
                if (include.matcher(name).matches()) {
                    for (Pattern exclude : excludes) {
                        if (exclude.matcher(name).matches()) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

    }
}
