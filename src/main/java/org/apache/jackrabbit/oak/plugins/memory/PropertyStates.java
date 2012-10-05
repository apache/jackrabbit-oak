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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.DATES;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.PATH;
import static org.apache.jackrabbit.oak.api.Type.PATHS;
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.REFERENCES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.api.Type.URI;
import static org.apache.jackrabbit.oak.api.Type.URIS;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCES;

public final class PropertyStates {
    private PropertyStates() {}

    @Nonnull
    public static PropertyState createProperty(String name, CoreValue value) {
        int type = value.getType();
        if (PropertyType.BINARY == type) {
           return binaryProperty(name, new ValueBasedBlob(value));
        }
        else {
            return createProperty(name, value.getString(), value.getType());
        }
    }

    @Nonnull
    public static PropertyState createProperty(String name, List<CoreValue> values) {
        if (values.isEmpty()) {
            return emptyProperty(name, STRINGS);
        }

        int type = values.get(0).getType();
        switch (type) {
            case PropertyType.STRING:
                List<String> strings = Lists.newArrayList();
                for (CoreValue cv : values) {
                    strings.add(cv.getString());
                }
                return stringProperty(name, strings);
            case PropertyType.BINARY:
                List<Blob> blobs = Lists.newArrayList();
                for (CoreValue cv : values) {
                    blobs.add(new ValueBasedBlob(cv));
                }
                return binaryPropertyFromBlob(name, blobs);
            case PropertyType.LONG:
                List<Long> longs = Lists.newArrayList();
                for (CoreValue cv : values) {
                    longs.add(cv.getLong());
                }
                return longProperty(name, longs);
            case PropertyType.DOUBLE:
                List<Double> doubles = Lists.newArrayList();
                for (CoreValue cv : values) {
                    doubles.add(cv.getDouble());
                }
                return doubleProperty(name, doubles);
            case PropertyType.BOOLEAN:
                List<Boolean> booleans = Lists.newArrayList();
                for (CoreValue cv : values) {
                    booleans.add(cv.getBoolean());
                }
                return booleanProperty(name, booleans);
            case PropertyType.DECIMAL:
                List<BigDecimal> decimals = Lists.newArrayList();
                for (CoreValue cv : values) {
                    decimals.add(cv.getDecimal());
                }
                return decimalProperty(name, decimals);
            default:
                List<String> vals = Lists.newArrayList();
                for (CoreValue cv : values) {
                    vals.add(cv.getString());
                }
                return new GenericsPropertyState(name, vals, Type.fromTag(type, true));
        }
    }

    public static PropertyState createProperty(String name, String value, int type) {
        switch (type) {
            case PropertyType.STRING:
                return new StringPropertyState(name, value);
            case PropertyType.BINARY:
                return new BinaryPropertyState(name, new StringBasedBlob(value));
            case PropertyType.LONG:
                return new LongPropertyState(name, SinglePropertyState.getLong(value));
            case PropertyType.DOUBLE:
                return new DoublePropertyState(name, StringPropertyState.getDouble(value));
            case PropertyType.BOOLEAN:
                return new BooleanPropertyState(name, StringPropertyState.getBoolean(value));
            case PropertyType.DECIMAL:
                return new DecimalPropertyState(name, StringPropertyState.getDecimal(value));
            default:
                return new GenericPropertyState(name, value, Type.fromTag(type, false));
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> PropertyState createProperty(String name, T value, Type<T> type) {
        switch (type.tag()) {
            case PropertyType.STRING: return type.isArray()
                ? stringProperty(name, (Iterable<String>) value)
                : stringProperty(name, (String) value);
            case PropertyType.BINARY: return type.isArray()
                ? binaryPropertyFromBlob(name, (Iterable<Blob>) value)
                : binaryProperty(name, (Blob) value);
            case PropertyType.LONG: return type.isArray()
                ? longProperty(name, (Iterable<Long>) value)
                : longProperty(name, (Long) value);
            case PropertyType.DOUBLE: return type.isArray()
                ? doubleProperty(name, (Iterable<Double>) value)
                : doubleProperty(name, (Double) value);
            case PropertyType.DATE: return type.isArray()
                ? dateProperty(name, (Iterable<String>) value)
                : dateProperty(name, (String) value);
            case PropertyType.BOOLEAN: return type.isArray()
                ? booleanProperty(name, (Iterable<Boolean>) value)
                : booleanProperty(name, (Boolean) value);
            case PropertyType.NAME: return type.isArray()
                ? nameProperty(name, (Iterable<String>) value)
                : nameProperty(name, (String) value);
            case PropertyType.PATH: return type.isArray()
                ? pathProperty(name, (Iterable<String>) value)
                : pathProperty(name, (String) value);
            case PropertyType.REFERENCE: return type.isArray()
                ? referenceProperty(name, (Iterable<String>) value)
                : referenceProperty(name, (String) value);
            case PropertyType.WEAKREFERENCE: return type.isArray()
                ? weakreferenceProperty(name, (Iterable<String>) value)
                : weakreferenceProperty(name, (String) value);
            case PropertyType.URI: return type.isArray()
                ? uriProperty(name, (Iterable<String>) value)
                : uriProperty(name, (String) value);
            case PropertyType.DECIMAL: return type.isArray()
                ? decimalProperty(name, (Iterable<BigDecimal>) value)
                : decimalProperty(name, (BigDecimal) value);
            default: throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    @Nonnull
    public static <T> PropertyState createProperty(String name, T value) {
        if (value instanceof String) {
            return stringProperty(name, (String) value);
        }
        else if (value instanceof Blob) {
            return binaryProperty(name, (Blob) value);
        }
        else if (value instanceof byte[]) {
            return binaryProperty(name, (byte[]) value);
        }
        else if (value instanceof Long) {
            return longProperty(name, (Long) value);
        }
        else if (value instanceof Integer) {
            return longProperty(name, (Integer) value);
        }
        else if (value instanceof Double) {
            return doubleProperty(name, (Double) value);
        }
        else if (value instanceof Boolean) {
            return booleanProperty(name, (Boolean) value);
        }
        else if (value instanceof BigDecimal) {
            return decimalProperty(name, (BigDecimal) value);
        }
        else {
            throw new IllegalArgumentException("Can't infer type of value of class '" + value.getClass() + '\'');
        }
    }

    public static PropertyState emptyProperty(String name, final Type<?> type) {
        if (!type.isArray()) {
            throw new IllegalArgumentException("Not an array type:" + type);
        }
        return new EmptyPropertyState(name) {
            @Override
            public Type<?> getType() {
                return type;
            }
        };
    }

    public static PropertyState stringProperty(String name, String value) {
        return new StringPropertyState(name, value);
    }

    public static PropertyState binaryProperty(String name, byte[] value) {
        return new BinaryPropertyState(name, new ArrayBasedBlob(value));
    }

    public static PropertyState longProperty(String name, long value) {
        return new LongPropertyState(name, value);
    }

    public static PropertyState doubleProperty(String name, double value) {
        return new DoublePropertyState(name, value);
    }

    public static PropertyState dateProperty(String name, String value) {
        return new GenericPropertyState(name, value, DATE);
    }

    public static PropertyState booleanProperty(String name, boolean value) {
        return new BooleanPropertyState(name, value);
    }

    public static PropertyState nameProperty(String name, String value) {
        return new GenericPropertyState(name, value, NAME);
    }

    public static PropertyState pathProperty(String name, String value) {
        return new GenericPropertyState(name, value, PATH);
    }

    public static PropertyState referenceProperty(String name, String value) {
        return new GenericPropertyState(name, value, REFERENCE);
    }

    public static PropertyState weakreferenceProperty(String name, String value) {
        return new GenericPropertyState(name, value, WEAKREFERENCE);
    }

    public static PropertyState uriProperty(String name, String value) {
        return new GenericPropertyState(name, value, URI);
    }

    public static PropertyState decimalProperty(String name, BigDecimal value) {
        return new DecimalPropertyState(name, value);
    }

    public static PropertyState binaryProperty(String name, Blob value) {
        return new BinaryPropertyState(name, value);
    }

    public static PropertyState stringProperty(String name, Iterable<String> values) {
        return new StringsPropertyState(name, Lists.newArrayList(values));
    }

    public static PropertyState binaryPropertyFromBlob(String name, Iterable<Blob> values) {
        return new BinariesPropertyState(name, Lists.newArrayList(values));
    }

    public static PropertyState longProperty(String name, Iterable<Long> values) {
        return new LongsPropertyState(name, Lists.newArrayList(values));
    }

    public static PropertyState doubleProperty(String name, Iterable<Double> values) {
        return new DoublesPropertyState(name, Lists.newArrayList(values));
    }

    public static PropertyState dateProperty(String name, Iterable<String> values) {
        return new GenericsPropertyState(name, Lists.newArrayList(values), DATES);
    }

    public static PropertyState booleanProperty(String name, Iterable<Boolean> values) {
        return new BooleansPropertyState(name, Lists.newArrayList(values));
    }

    public static PropertyState nameProperty(String name, Iterable<String> values) {
        return new GenericsPropertyState(name, Lists.newArrayList(values), NAMES);
    }

    public static PropertyState pathProperty(String name, Iterable<String> values) {
        return new GenericsPropertyState(name, Lists.newArrayList(values), PATHS);
    }

    public static PropertyState referenceProperty(String name, Iterable<String> values) {
        return new GenericsPropertyState(name, Lists.newArrayList(values), REFERENCES);
    }

    public static PropertyState weakreferenceProperty(String name, Iterable<String> values) {
        return new GenericsPropertyState(name, Lists.newArrayList(values), WEAKREFERENCES);
    }

    public static PropertyState uriProperty(String name, Iterable<String> values) {
        return new GenericsPropertyState(name, Lists.newArrayList(values), URIS);
    }

    public static PropertyState decimalProperty(String name, Iterable<BigDecimal> values) {
        return new DecimalsPropertyState(name, Lists.newArrayList(values));
    }

    public static PropertyState binaryPropertyFromArray(String name, Iterable<byte[]> values) {
        List<Blob> blobs = Lists.newArrayList();
        for (byte[] data : values) {
            blobs.add(new ArrayBasedBlob(data));
        }
        return new BinariesPropertyState(name, blobs);
    }
}
