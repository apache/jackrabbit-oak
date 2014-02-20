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
package org.apache.jackrabbit.oak.kernel;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DOUBLE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Utility class for serializing node and property states to JSON.
 */
public class JsonSerializer {

    private static final JsonFilter DEFAULT_FILTER =
            new JsonFilter("{\"properties\":[\"*\", \"-:childNodeCount\"]}");

    private final JsopBuilder json;

    private final int depth;

    private final long offset;

    private final int maxChildNodes;

    private final JsonFilter filter;

    private final BlobSerializer blobs;

    private JsonSerializer(
            JsopBuilder json, int depth, long offset, int maxChildNodes,
            JsonFilter filter, BlobSerializer blobs) {
        this.json = checkNotNull(json);
        this.depth = depth;
        this.offset = offset;
        this.maxChildNodes = maxChildNodes;
        this.filter = checkNotNull(filter);
        this.blobs = checkNotNull(blobs);
    }

    JsonSerializer(
            int depth, long offset, int maxChildNodes,
            String filter, BlobSerializer blobs) {
        this(new JsopBuilder(), depth, offset, maxChildNodes,
                new JsonFilter(filter), blobs);
    }

    public JsonSerializer(JsopBuilder json, BlobSerializer blobs) {
        this(json, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
                DEFAULT_FILTER, blobs);
    }

    protected JsonSerializer getChildSerializer() {
        return new JsonSerializer(
                json, depth - 1, 0, maxChildNodes, filter, blobs);
    }

    void serialize(NodeState node) {
        json.object();

        for (PropertyState property : node.getProperties()) {
            String name = property.getName();
            if (filter.includeProperty(name)) {
                json.key(name);
                serialize(property);
            }
        }

        if (filter.includeProperty(":childNodeCount")) {
            json.key(":childNodeCount");
            json.value(node.getChildNodeCount(Integer.MAX_VALUE));
        }

        int index = 0;
        int count = 0;
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
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

    void serialize(PropertyState property, Type<?> type, int index) {
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

}
