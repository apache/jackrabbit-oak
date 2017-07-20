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

package org.apache.jackrabbit.oak.json;

import java.util.List;

import javax.jcr.PropertyType;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class JsonDeserializer {
    private final BlobDeserializer blobHandler;
    private final NodeBuilder builder;

    public JsonDeserializer(BlobDeserializer blobHandler, NodeBuilder builder) {
        this.blobHandler = blobHandler;
        this.builder = builder;
    }

    public JsonDeserializer(BlobDeserializer blobHandler) {
        this(blobHandler, EMPTY_NODE.builder());
    }

    public NodeState deserialize(String json){
        JsopReader reader = new JsopTokenizer(json);
        reader.read('{');
        NodeState state = deserialize(reader);
        reader.read(JsopReader.END);
        return state;
    }

    public NodeState deserialize(JsopReader reader){
        readNode(reader, builder);
        reader.read('}');
        return builder.getNodeState();
    }

    private void readNode(JsopReader reader, NodeBuilder builder) {
        do {
            String key = reader.readString();
            reader.read(':');
            if (reader.matches('{')) {
                readNode(reader, builder.child(key));
                reader.read('}');
            } else if (reader.matches('[')){
                builder.setProperty(readArrayProperty(key, reader));
            } else {
                builder.setProperty(readProperty(key, reader));
            }
        } while (reader.matches(','));
    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private PropertyState readProperty(String name, JsopReader reader) {
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            try {
                return new LongPropertyState(name, Long.parseLong(number));
            } catch (NumberFormatException e) {
                return new DoublePropertyState(name, Double.parseDouble(number));
            }
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            if (jsonString.startsWith(TypeCodes.EMPTY_ARRAY)) {
                int type = PropertyType.valueFromName(
                        jsonString.substring(TypeCodes.EMPTY_ARRAY.length()));
                return PropertyStates.createProperty(
                        name, emptyList(), Type.fromTag(type, true));
            }
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    return  BinaryPropertyState.binaryProperty(
                            name, blobHandler.deserialize(value));
                } else {
                    return createProperty(name, value, type);
                }
            } else {
                return StringPropertyState.stringProperty(
                        name, jsonString);
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    /**
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private PropertyState readArrayProperty(String name, JsopReader reader) {
        int type = PropertyType.STRING;
        List<Object> values = Lists.newArrayList();
        while (!reader.matches(']')) {
            if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                try {
                    type = PropertyType.LONG;
                    values.add(Long.parseLong(number));
                } catch (NumberFormatException e) {
                    type = PropertyType.DOUBLE;
                    values.add(Double.parseDouble(number));
                }
            } else if (reader.matches(JsopReader.TRUE)) {
                type = PropertyType.BOOLEAN;
                values.add(true);
            } else if (reader.matches(JsopReader.FALSE)) {
                type = PropertyType.BOOLEAN;
                values.add(false);
            } else if (reader.matches(JsopReader.STRING)) {
                String jsonString = reader.getToken();
                int split = TypeCodes.split(jsonString);
                if (split != -1) {
                    type = TypeCodes.decodeType(split, jsonString);
                    String value = TypeCodes.decodeName(split, jsonString);
                    if (type == PropertyType.BINARY) {
                        values.add(blobHandler.deserialize(value));
                    } else if (type == PropertyType.DOUBLE) {
                        values.add(Conversions.convert(value).toDouble());
                    } else if (type == PropertyType.DECIMAL) {
                        values.add(Conversions.convert(value).toDecimal());
                    } else {
                        values.add(value);
                    }
                } else {
                    type = PropertyType.STRING;
                    values.add(jsonString);
                }
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
            reader.matches(',');
        }
        return createProperty(name, values, Type.fromTag(type, true));
    }


}
