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
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.kernel.KernelBlob;
import org.apache.jackrabbit.oak.kernel.TypeCodes;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * Utility class for creating {@link PropertyState} instances.
 */
public final class PropertyStates {
    private PropertyStates() {}

    /**
     * Create a {@code PropertyState} based on a {@link Value}. The
     * {@link Type} of the property state is determined by the
     * type of the value.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state
     * @throws RepositoryException forwarded from {@code value}
     */
    @Nonnull
    public static PropertyState createProperty(String name, Value value) throws RepositoryException {
        int type = value.getType();
        switch (type) {
            case PropertyType.STRING:
                return StringPropertyState.stringProperty(name, value.getString());
            case PropertyType.BINARY:
                return BinaryPropertyState.binaryProperty(name, value);
            case PropertyType.LONG:
                return LongPropertyState.createLongProperty(name, value.getLong());
            case PropertyType.DOUBLE:
                return DoublePropertyState.doubleProperty(name, value.getDouble());
            case PropertyType.DATE:
                return LongPropertyState.createDateProperty(name, value.getLong());
            case PropertyType.BOOLEAN:
                return BooleanPropertyState.booleanProperty(name, value.getBoolean());
            case PropertyType.DECIMAL:
                return DecimalPropertyState.decimalProperty(name, value.getDecimal());
            default:
                return new GenericPropertyState(name, value.getString(), Type.fromTag(type, false));
        }
    }

    /**
     * Create a multi valued {@code PropertyState} based on a list of
     * {@link Value} instances. The {@link Type} of the property is determined
     * by the type of the first value in the list or {@link Type#STRING} if the
     * list is empty.
     *
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state
     * @throws RepositoryException forwarded from {@code value}
     */
    @Nonnull
    public static PropertyState createProperty(String name, Iterable<Value> values) throws RepositoryException {
        Value first = Iterables.getFirst(values, null);
        if (first == null) {
            return EmptyPropertyState.emptyProperty(name, STRINGS);
        }

        int type = first.getType();
        switch (type) {
            case PropertyType.STRING:
                List<String> strings = Lists.newArrayList();
                for (Value value : values) {
                    strings.add(value.getString());
                }
                return MultiStringPropertyState.stringProperty(name, strings);
            case PropertyType.BINARY:
                List<Blob> blobs = Lists.newArrayList();
                for (Value value : values) {
                    blobs.add(new ValueBasedBlob(value));
                }
                return MultiBinaryPropertyState.binaryPropertyFromBlob(name, blobs);
            case PropertyType.LONG:
                List<Long> longs = Lists.newArrayList();
                for (Value value : values) {
                    longs.add(value.getLong());
                }
                return MultiLongPropertyState.createLongProperty(name, longs);
            case PropertyType.DOUBLE:
                List<Double> doubles = Lists.newArrayList();
                for (Value value : values) {
                    doubles.add(value.getDouble());
                }
                return MultiDoublePropertyState.doubleProperty(name, doubles);
            case PropertyType.DATE:
                List<Long> dates = Lists.newArrayList();
                for (Value value : values) {
                    dates.add(value.getLong());
                }
                return MultiLongPropertyState.createDatePropertyFromLong(name, dates);
            case PropertyType.BOOLEAN:
                List<Boolean> booleans = Lists.newArrayList();
                for (Value value : values) {
                    booleans.add(value.getBoolean());
                }
                return MultiBooleanPropertyState.booleanProperty(name, booleans);
            case PropertyType.DECIMAL:
                List<BigDecimal> decimals = Lists.newArrayList();
                for (Value value : values) {
                    decimals.add(value.getDecimal());
                }
                return MultiDecimalPropertyState.decimalProperty(name, decimals);
            default:
                List<String> vals = Lists.newArrayList();
                for (Value value : values) {
                    vals.add(value.getString());
                }
                return new MultiGenericPropertyState(name, vals, Type.fromTag(type, true));
        }
    }

    /**
     * Create a {@code PropertyState} from a string.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @param type  The type of the property state
     * @return  The new property state
     */
    @Nonnull
    public static PropertyState createProperty(String name, String value, int type) {
        switch (type) {
            case PropertyType.STRING:
                return StringPropertyState.stringProperty(name, value);
            case PropertyType.BINARY:
                return  BinaryPropertyState.binaryProperty(name, Conversions.convert(value).toBinary());
            case PropertyType.LONG:
                return LongPropertyState.createLongProperty(name, Conversions.convert(value).toLong());
            case PropertyType.DOUBLE:
                return DoublePropertyState.doubleProperty(name, Conversions.convert(value).toDouble());
            case PropertyType.DATE:
                return LongPropertyState.createDateProperty(name, value);
            case PropertyType.BOOLEAN:
                return BooleanPropertyState.booleanProperty(name, Conversions.convert(value).toBoolean());
            case PropertyType.DECIMAL:
                return DecimalPropertyState.decimalProperty(name, Conversions.convert(value).toDecimal());
            default:
                return new GenericPropertyState(name, value, Type.fromTag(type, false));
        }
    }

    /**
     * Create a {@code PropertyState}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @param type  The type of the property state
     * @return  The new property state
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> PropertyState createProperty(String name, T value, Type<T> type) {
        switch (type.tag()) {
            case PropertyType.STRING:
                return type.isArray()
                ? MultiStringPropertyState.stringProperty(name, (Iterable<String>) value)
                : StringPropertyState.stringProperty(name, (String) value);
            case PropertyType.BINARY:
                return type.isArray()
                ? MultiBinaryPropertyState.binaryPropertyFromBlob(name, (Iterable<Blob>) value)
                : BinaryPropertyState.binaryProperty(name, (Blob) value);
            case PropertyType.LONG:
                return type.isArray()
                ? MultiLongPropertyState.createLongProperty(name, (Iterable<Long>) value)
                : LongPropertyState.createLongProperty(name, (Long) value);
            case PropertyType.DOUBLE:
                return type.isArray()
                ? MultiDoublePropertyState.doubleProperty(name, (Iterable<Double>) value)
                : DoublePropertyState.doubleProperty(name, (Double) value);
            case PropertyType.DATE:
                return type.isArray()
                ? MultiLongPropertyState.createDateProperty(name, (Iterable<String>) value)
                : LongPropertyState.createDateProperty(name, (String) value);
            case PropertyType.BOOLEAN:
                return type.isArray()
                ? MultiBooleanPropertyState.booleanProperty(name, (Iterable<Boolean>) value)
                : BooleanPropertyState.booleanProperty(name, (Boolean) value);
            case PropertyType.NAME:
                return type.isArray()
                ? MultiGenericPropertyState.nameProperty(name, (Iterable<String>) value)
                : GenericPropertyState.nameProperty(name, (String) value);
            case PropertyType.PATH:
                return type.isArray()
                ? MultiGenericPropertyState.pathProperty(name, (Iterable<String>) value)
                : GenericPropertyState.pathProperty(name, (String) value);
            case PropertyType.REFERENCE:
                return type.isArray()
                ? MultiGenericPropertyState.referenceProperty(name, (Iterable<String>) value)
                : GenericPropertyState.referenceProperty(name, (String) value);
            case PropertyType.WEAKREFERENCE:
                return type.isArray()
                ? MultiGenericPropertyState.weakreferenceProperty(name, (Iterable<String>) value)
                : GenericPropertyState.weakreferenceProperty(name, (String) value);
            case PropertyType.URI:
                return type.isArray()
                ? MultiGenericPropertyState.uriProperty(name, (Iterable<String>) value)
                : GenericPropertyState.uriProperty(name, (String) value);
            case PropertyType.DECIMAL:
                return type.isArray()
                ? MultiDecimalPropertyState.decimalProperty(name, (Iterable<BigDecimal>) value)
                : DecimalPropertyState.decimalProperty(name, (BigDecimal) value);
            default: throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    /**
     * Create a {@code PropertyState} where the {@link Type} of the property state
     * is inferred from the runtime type of {@code T} according to the mapping
     * established through {@code Type}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state
     */
    @Nonnull
    public static <T> PropertyState createProperty(String name, T value) {
        if (value instanceof String) {
            return StringPropertyState.stringProperty(name, (String) value);
        } else if (value instanceof Blob) {
            return BinaryPropertyState.binaryProperty(name, (Blob) value);
        } else if (value instanceof byte[]) {
            return BinaryPropertyState.binaryProperty(name, (byte[]) value);
        } else if (value instanceof Long) {
            return LongPropertyState.createLongProperty(name, (Long) value);
        } else if (value instanceof Integer) {
            return LongPropertyState.createLongProperty(name, (long) (Integer) value);
        } else if (value instanceof Double) {
            return DoublePropertyState.doubleProperty(name, (Double) value);
        } else if (value instanceof Boolean) {
            return BooleanPropertyState.booleanProperty(name, (Boolean) value);
        } else if (value instanceof BigDecimal) {
            return DecimalPropertyState.decimalProperty(name, (BigDecimal) value);
        } else {
            throw new IllegalArgumentException("Can't infer type of value of class '" + value.getClass() + '\'');
        }
    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @param kernel  {@link MicroKernel} instance used to resolve binaries
     * @return  The new property state of type {@link Type#DECIMALS}
     */
    public static PropertyState readProperty(String name, JsopReader reader, MicroKernel kernel) {
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            return createProperty(name, number, PropertyType.LONG);
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    return  BinaryPropertyState.binaryProperty(name, new KernelBlob(value, kernel));
                } else {
                    return createProperty(name, value, type);
                }
            } else {
                return StringPropertyState.stringProperty(name, jsonString);
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    /**
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @param kernel  {@link MicroKernel} instance used to resolve binaries
     * @return  The new property state of type {@link Type#DECIMALS}
     */
    public static PropertyState readArrayProperty(String name, JsopReader reader, MicroKernel kernel) {
        int type = PropertyType.STRING;
        List<Object> values = Lists.newArrayList();
        while (!reader.matches(']')) {
            if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                type = PropertyType.LONG;
                values.add(Conversions.convert(number).toLong());
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
                        values.add(new KernelBlob(value, kernel));
                    } else if(type == PropertyType.DOUBLE) {
                        values.add(Conversions.convert(value).toDouble());
                    } else if(type == PropertyType.DECIMAL) {
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
        return createProperty(name, values, (Type<Object>) Type.fromTag(type, true));
    }
}
