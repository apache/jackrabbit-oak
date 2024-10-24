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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.jetbrains.annotations.NotNull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for serializing node and property states to JSON.
 */
public class JsonSerializer {
    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);

    public static final String DEFAULT_FILTER_EXPRESSION =
        "{\"properties\":[\"*\", \"-:childNodeCount\"]}";

    private static final JsonFilter DEFAULT_FILTER = new JsonFilter(DEFAULT_FILTER_EXPRESSION);

    private static final String ERROR_JSON_KEY = "_error";
    private static final String ERROR_JSON_VALUE_PREFIX = "ERROR: ";

    private final JsopWriter json;

    private final int depth;

    private final long offset;

    private final int maxChildNodes;

    private final JsonFilter filter;

    private final BlobSerializer blobs;

    private final boolean catchExceptions;

    private JsonSerializer(
            JsopWriter json, int depth, long offset, int maxChildNodes,
            JsonFilter filter, BlobSerializer blobs, boolean catchExceptions) {
        this.json = requireNonNull(json);
        this.depth = depth;
        this.offset = offset;
        this.maxChildNodes = maxChildNodes;
        this.filter = requireNonNull(filter);
        this.blobs = requireNonNull(blobs);
        this.catchExceptions = catchExceptions;
    }

    public JsonSerializer(
            int depth, long offset, int maxChildNodes,
            String filter, BlobSerializer blobs) {
        this(new JsopBuilder(), depth, offset, maxChildNodes,
                new JsonFilter(filter), blobs, false);
    }

    public JsonSerializer(JsopWriter json,
            int depth, long offset, int maxChildNodes,
            String filter, BlobSerializer blobs) {
        this(json, depth, offset, maxChildNodes,
                new JsonFilter(filter), blobs, false);
    }

    public JsonSerializer(JsopWriter json,
                          int depth, long offset, int maxChildNodes,
                          String filter, BlobSerializer blobs, boolean catchExceptions) {
        this(json, depth, offset, maxChildNodes,
            new JsonFilter(filter), blobs, catchExceptions);
    }

    public JsonSerializer(JsopWriter json, BlobSerializer blobs) {
        this(json, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                DEFAULT_FILTER, blobs, false);
    }

    public JsonSerializer(JsopWriter json, String filter, BlobSerializer blobs) {
        this(json, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                new JsonFilter(filter), blobs, false);
    }

    protected JsonSerializer getChildSerializer() {
        return new JsonSerializer(
                json, depth - 1, 0, maxChildNodes, filter, blobs, catchExceptions);
    }

    public void serialize(NodeState node) {
        serialize(node, "");
    }

    public void serialize(NodeState node, String basePath) {
        json.object();

        try {
            for (PropertyState property : node.getProperties()) {
                String name = property.getName();
                if (filter.includeProperty(name)) {
                    json.key(name);
                    try {
                        serialize(property);
                    } catch (Throwable t) {
                        if (catchExceptions) {
                            String message = "Cannot read property value " + basePath + "/" + name + " : " + t.getMessage();
                            log.error(message);
                            json.value(ERROR_JSON_VALUE_PREFIX + message);
                        } else {
                            throw t;
                        }
                    }
                }
            }

            int index = 0;
            int count = 0;
            for (ChildNodeEntry child : getChildNodeEntries(node, basePath)) {
                String name = child.getName();
                if (filter.includeNode(name) && index++ >= offset) {
                    if (count++ >= maxChildNodes) {
                        break;
                    }

                    json.key(name);
                    if (depth > 0) {
                        getChildSerializer().serialize(child.getNodeState(), basePath + "/" + name);
                    } else {
                        json.object();
                        json.endObject();
                    }
                }
            }
        } catch (Throwable t) {
            if (catchExceptions) {
                String message = "Cannot read node " + basePath + " : " + t.getMessage();
                log.error(message);
                json.key(ERROR_JSON_KEY);
                json.value(ERROR_JSON_VALUE_PREFIX + message);
            } else {
                throw t;
            }
        }

        json.endObject();
    }

    private Iterable<? extends ChildNodeEntry> getChildNodeEntries(NodeState node, String basePath) {
        PropertyState order = node.getProperty(":childOrder");
        if (order != null) {
            List<String> names = ImmutableList.copyOf(order.getValue(NAMES));
            List<ChildNodeEntry> entries = new ArrayList<>(names.size());
            for (String name : names) {
                try {
                    entries.add(new MemoryChildNodeEntry(name, node.getChildNode(name)));
                } catch (Throwable t) {
                    if (catchExceptions) {
                        String message = "Cannot read node " + basePath + "/" + name + " : " + t.getMessage();
                        log.error(message);

                        // return a placeholder child node entry for tracking the error into the JSON
                        entries.add(new MemoryChildNodeEntry(name, new AbstractNodeState() {
                            @Override
                            public boolean exists() {
                                return true;
                            }

                            @NotNull
                            @Override
                            public Iterable<? extends PropertyState> getProperties() {
                                return Collections.singleton(new StringPropertyState(ERROR_JSON_KEY, ERROR_JSON_VALUE_PREFIX + message));
                            }

                            @Override
                            public boolean hasChildNode(@NotNull String name) {
                                return false;
                            }

                            @NotNull
                            @Override
                            public NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
                                return EmptyNodeState.MISSING_NODE;
                            }

                            @NotNull
                            @Override
                            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                                return Collections.EMPTY_LIST;
                            }

                            @NotNull
                            @Override
                            public NodeBuilder builder() {
                                return new ReadOnlyBuilder(this);
                            }
                        }));
                    } else {
                        throw t;
                    }
                }
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
            json.value(property.getValue(BOOLEAN, index));
        } else if (type == LONG) {
            json.value(property.getValue(LONG, index));
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

        private final List<Pattern> nodeExcludes = new ArrayList<>();

        private final List<Pattern> propertyIncludes = newArrayList(EVERYTHING);

        private final List<Pattern> propertyExcludes = new ArrayList<>();

        JsonFilter(String filter) {
            JsopTokenizer tokenizer = new JsopTokenizer(filter);
            tokenizer.read('{');
            for (boolean first = true; !tokenizer.matches('}'); first = false) {
                if (!first) {
                    tokenizer.read(',');
                }
                String key = tokenizer.readString();
                tokenizer.read(':');

                List<Pattern> includes = new ArrayList<>();
                List<Pattern> excludes = new ArrayList<>();
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
