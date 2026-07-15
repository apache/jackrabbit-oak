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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class JsonDeserializer {
    /**
     * Provides support for inferring types for some common property name and types
     */
    private static final Set<String> NAME_PROPS = Set.of(JCR_PRIMARYTYPE, JCR_MIXINTYPES);
    private static final String ORDERABLE_TYPE = NT_UNSTRUCTURED;
    private static final String OAK_CHILD_ORDER = ":childOrder";

    private static Type<?> inferPropertyType(String propertyName, String jsonString) {
        if (NAME_PROPS.contains(propertyName) && hasSingleColon(jsonString)) {
            return Type.NAME;
        }
        return Type.UNDEFINED;
    }

    private static boolean hasOrderableChildren(NodeBuilder builder) {
        PropertyState primaryType = builder.getProperty(JCR_PRIMARYTYPE);
        return primaryType != null && ORDERABLE_TYPE.equals(primaryType.getValue(Type.NAME));
    }

    private static boolean hasSingleColon(String jsonString) {
        // In case the primaryType was encoded then it would be like
        // "nam:oak:Unstructured". So check if there is only one occurrence
        // of ':'.
        int colonCount = 0;
        for (int i = 0; i < jsonString.length() && colonCount < 2; i++) {
            if (jsonString.charAt(i) == ':') {
                colonCount += 1;
            }
        }
        return colonCount == 1;
    }

    private final BlobDeserializer blobHandler;

    public JsonDeserializer(BlobDeserializer blobHandler) {
        this.blobHandler = blobHandler;
    }

    public NodeState deserialize(String json) {
        return deserialize(json, 0);
    }

    public NodeState deserialize(String line, int pos) {
        JsopReader reader = new JsopTokenizer(line, pos);
        reader.read('{');
        NodeState state = deserialize(reader);
        reader.read(JsopReader.END);
        return state;
    }

    public NodeState deserialize(JsopReader reader) {
        NodeBuilder builder = EMPTY_NODE.builder();
        readNode(reader, builder);
        return builder.getNodeState();
    }

    private void readNode(JsopReader reader, NodeBuilder builder) {
        List<String> childNames = new ArrayList<>();
        if (!reader.matches('}')) {
            do {
                String key = reader.readString();
                reader.read(':');
                if (reader.matches('{')) {
                    childNames.add(key);
                    readNode(reader, builder.child(key));
                } else if (reader.matches('[')) {
                    builder.setProperty(readArrayProperty(key, reader));
                } else {
                    builder.setProperty(readProperty(key, reader));
                }
            } while (reader.matches(','));
            reader.read('}');
        }

        if (hasOrderableChildren(builder) && !builder.hasProperty(OAK_CHILD_ORDER)) {
            builder.setProperty(OAK_CHILD_ORDER, childNames, Type.NAMES);
        }

    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private PropertyState readProperty(String name, JsopReader reader) {
        // Test for the most common types first
        if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            Type<?> inferredType = inferPropertyType(name, jsonString);
            if (jsonString.startsWith(TypeCodes.EMPTY_ARRAY)) {
                int type = PropertyType.valueFromName(jsonString.substring(TypeCodes.EMPTY_ARRAY.length()));
                return PropertyStates.createProperty(name, List.of(), Type.fromTag(type, true));
            }
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    return BinaryPropertyState.binaryProperty(name, blobHandler.deserialize(value));
                } else {
                    //It can happen that a value like oak:Unstructured is also interpreted
                    //as type code. So if oakType is not undefined then use raw value
                    //Also default to STRING in case of UNDEFINED
                    if (type == PropertyType.UNDEFINED) {
                        Type<?> oakType = inferredType != Type.UNDEFINED ? inferredType : Type.STRING;
                        return createProperty(name, jsonString, oakType);
                    }
                    return createProperty(name, value, type);
                }
            } else {
                Type<?> oakType = inferredType != Type.UNDEFINED ? inferredType : Type.STRING;
                return createProperty(name, jsonString, oakType);
            }
        } else if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            Long maybeLong = LongUtils.tryParse(number);
            if (maybeLong == null) {
                return new DoublePropertyState(name, Double.parseDouble(number));
            } else {
                return new LongPropertyState(name, maybeLong);
            }
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
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
        List<Object> values = new ArrayList<>();
        while (!reader.matches(']')) {
            // Test for the most common types first
            if (reader.matches(JsopReader.STRING)) {
                String jsonString = reader.getToken();
                Type<?> inferredType = inferPropertyType(name, jsonString);
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
                        if (type == PropertyType.UNDEFINED) {
                            //If determine type is undefined then check if inferred type is defined
                            //else default to STRING
                            type = inferredType != Type.UNDEFINED ? inferredType.tag() : PropertyType.STRING;
                            values.add(jsonString);
                        } else {
                            values.add(value);
                        }
                    }
                } else {
                    type = inferredType != Type.UNDEFINED ? inferredType.tag() : PropertyType.STRING;
                    values.add(jsonString);
                }
            } else if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                Long maybeLong = LongUtils.tryParse(number);
                if (maybeLong == null) {
                    type = PropertyType.DOUBLE;
                    values.add(Double.parseDouble(number));
                } else {
                    type = PropertyType.LONG;
                    values.add(maybeLong);
                }
            } else if (reader.matches(JsopReader.TRUE)) {
                type = PropertyType.BOOLEAN;
                values.add(Boolean.TRUE);
            } else if (reader.matches(JsopReader.FALSE)) {
                type = PropertyType.BOOLEAN;
                values.add(Boolean.FALSE);
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
            reader.matches(',');
        }
        return createProperty(name, values, Type.fromTag(type, true));
    }
}
