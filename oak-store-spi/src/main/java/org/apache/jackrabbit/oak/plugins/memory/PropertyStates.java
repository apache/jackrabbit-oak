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
import java.util.Calendar;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.OakValue;
import org.apache.jackrabbit.util.ISO8601;

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
                return StringPropertyState.stringProperty(name, getString(value, type));
            case PropertyType.BINARY:
                return BinaryPropertyState.binaryProperty(name, value);
            case PropertyType.LONG:
                return LongPropertyState.createLongProperty(name, value.getLong());
            case PropertyType.DOUBLE:
                return DoublePropertyState.doubleProperty(name, value.getDouble());
            case PropertyType.BOOLEAN:
                return BooleanPropertyState.booleanProperty(name, value.getBoolean());
            case PropertyType.DECIMAL:
                return DecimalPropertyState.decimalProperty(name, value.getDecimal());
            default:
                return new GenericPropertyState(name, getString(value, type), Type.fromTag(type, false));
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
    public static PropertyState createProperty(
            String name, Iterable<Value> values)
            throws RepositoryException {
        int type = PropertyType.STRING;
        Value first = Iterables.getFirst(values, null);
        if (first != null) {
            type = first.getType();
        }
        return createProperty(name, values, type);
    }

    public static PropertyState createProperty(
            String name, Iterable<Value> values, int type)
            throws RepositoryException {
        switch (type) {
            case PropertyType.STRING:
                List<String> strings = Lists.newArrayList();
                for (Value value : values) {
                    strings.add(getString(value, type));
                }
                return MultiStringPropertyState.stringProperty(name, strings);
            case PropertyType.BINARY:
                List<Blob> blobs = Lists.newArrayList();
                for (Value value : values) {
                    blobs.add(AbstractPropertyState.getBlob(value));
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
                    vals.add(getString(value, type));
                }
                return new MultiGenericPropertyState(name, vals, Type.fromTag(type, true));
        }
    }

    private static String getString(Value value, int type) throws RepositoryException {
        if (value instanceof OakValue) {
            return ((OakValue) value).getOakString();
        }
        else if (type == PropertyType.NAME || type == PropertyType.PATH) {
            throw new IllegalArgumentException("Cannot create name of path property state from Value " +
                    "of class '" + value.getClass() + '\'');
        } else {
            return value.getString();
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
    public static PropertyState createProperty(String name, Object value, Type<?> type) {
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
                ? MultiGenericPropertyState.dateProperty(name, (Iterable<String>) value)
                : GenericPropertyState.dateProperty(name, (String) value);
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
        } else if (value instanceof Calendar) {
            return GenericPropertyState.dateProperty(name, ISO8601.format((Calendar) value));
        } else if (value instanceof Boolean) {
            return BooleanPropertyState.booleanProperty(name, (Boolean) value);
        } else if (value instanceof BigDecimal) {
            return DecimalPropertyState.decimalProperty(name, (BigDecimal) value);
        } else {
            throw new IllegalArgumentException("Can't infer type of value of class '" + value.getClass() + '\'');
        }
    }

    public static PropertyState convert(PropertyState state, Type<?> type) {
        if (type == state.getType()
                || (type == Type.UNDEFINED && !state.isArray())
                || (type == Type.UNDEFINEDS && state.isArray())) {
            return state;
        } else {
            return createProperty(state.getName(), state.getValue(type), type);
        }
    }

}
