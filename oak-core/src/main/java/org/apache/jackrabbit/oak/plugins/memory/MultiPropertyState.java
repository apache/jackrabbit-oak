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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Abstract base class for multi valued {@code PropertyState} implementations.
 */
abstract class MultiPropertyState<T> extends EmptyPropertyState {
    protected final List<T> values;

    /**
     * Create a new property state with the given {@code name}
     * and {@code values}
     * @param name  The name of the property state.
     * @param values  The values of the property state.
     */
    protected MultiPropertyState(String name, List<T> values) {
        super(name);
        this.values = values;
    }

    /**
     * @return  {@code Iterable} of the string representations of the values
     * of the property state.
     */
    protected abstract Iterable<String> getStrings();

    /**
     * @param index
     * @return  String representation of the value at {@code index }of the
     * property state.
     */
    protected abstract String getString(int index);

    /**
     * @return  The values of this property state as {@link Blob}s
     */
    protected Iterable<Blob> getBlobs() {
        return Iterables.transform(getStrings(), new Function<String, Blob>() {
            @Override
            public Blob apply(String value) {
                return Conversions.convert(value).toBinary();
            }
        });
    }

    /**
     * @return  The values of this property state as {@code Long}s
     */
    protected Iterable<Long> getLongs() {
        return Iterables.transform(getStrings(), new Function<String, Long>() {
            @Override
            public Long apply(String value) {
                return Conversions.convert(value).toLong();
            }
        });
    }

    /**
     * @return  The values of this property state as {@code Double}s
     */
    protected Iterable<Double> getDoubles() {
        return Iterables.transform(getStrings(), new Function<String, Double>() {
            @Override
            public Double apply(String value) {
                return Conversions.convert(value).toDouble();
            }
        });
    }

    /**
     * @return  The values of this property state as {@code Booleans}s
     */
    protected Iterable<Boolean> getBooleans() {
        return Iterables.transform(getStrings(), new Function<String, Boolean>() {
            @Override
            public Boolean apply(String value) {
                return Conversions.convert(value).toBoolean();
            }
        });
    }

    /**
     * @return  The values of this property state as {@code BigDecimal}s
     */
    protected Iterable<BigDecimal> getDecimals() {
        return Iterables.transform(getStrings(), new Function<String, BigDecimal>() {
            @Override
            public BigDecimal apply(String value) {
                return Conversions.convert(value).toDecimal();
            }
        });
    }

    /**
     * @param index
     * @return  The value at the given {@code index} as {@link Blob}
     */
    protected Blob getBlob(int index) {
        return Conversions.convert(getString(index)).toBinary();
    }

    /**
     * @param index
     * @return  The value at the given {@code index} as {@code long}
     */
    protected long getLong(int index) {
        return Conversions.convert(getString(index)).toLong();
    }

    /**
     * @param index
     * @return  The value at the given {@code index} as {@code double}
     */
    protected double getDouble(int index) {
        return Conversions.convert(getString(index)).toDouble();
    }

    /**
     * @param index
     * @return  The value at the given {@code index} as {@code boolean}
     */
    protected boolean getBoolean(int index) {
        return Conversions.convert(getString(index)).toBoolean();
    }

    /**
     * @param index
     * @return  The value at the given {@code index} as {@code BigDecimal}
     */
    protected BigDecimal getDecimal(int index) {
        return Conversions.convert(getString(index)).toDecimal();
    }

    /**
     * @throws IllegalArgumentException if {@code type} is not one of the
     * values defined in {@link Type} or if {@code type.isArray()} is {@code false}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        checkArgument(type.isArray(), "Type must not be an array type");
        switch (type.tag()) {
            case PropertyType.STRING: return (T) getStrings();
            case PropertyType.BINARY: return (T) getBlobs();
            case PropertyType.LONG: return (T) getLongs();
            case PropertyType.DOUBLE: return (T) getDoubles();
            case PropertyType.DATE: return (T) getStrings();
            case PropertyType.BOOLEAN: return (T) getBooleans();
            case PropertyType.NAME: return (T) getStrings();
            case PropertyType.PATH: return (T) getStrings();
            case PropertyType.REFERENCE: return (T) getStrings();
            case PropertyType.WEAKREFERENCE: return (T) getStrings();
            case PropertyType.URI: return (T) getStrings();
            case PropertyType.DECIMAL: return (T) getDecimals();
            default: throw new IllegalArgumentException("Invalid type:" + type);
        }
    }

    /**
     * @throws IllegalArgumentException if {@code type} is not one of the
     * values defined in {@link Type} or if {@code type.isArray()} is {@code true}
     * @throws IndexOutOfBoundsException if {@code index >= count()}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        checkArgument(!type.isArray(), "Type must not be an array type");
        if (index >= count()) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        switch (type.tag()) {
            case PropertyType.STRING: return (T) getString(index);
            case PropertyType.BINARY: return (T) getBlob(index);
            case PropertyType.LONG: return (T) (Long) getLong(index);
            case PropertyType.DOUBLE: return (T) (Double) getDouble(index);
            case PropertyType.DATE: return (T) getString(index);
            case PropertyType.BOOLEAN: return (T) (Boolean) getBoolean(index);
            case PropertyType.NAME: return (T) getString(index);
            case PropertyType.PATH: return (T) getString(index);
            case PropertyType.REFERENCE: return (T) getString(index);
            case PropertyType.WEAKREFERENCE: return (T) getString(index);
            case PropertyType.URI: return (T) getString(index);
            case PropertyType.DECIMAL: return (T) getString(index);
            default: throw new IllegalArgumentException("Invalid type:" + type);
        }
    }

    @Override
    public final int count() {
        return values.size();
    }

    @Override
    public long size(int index) {
        return getString(index).length();
    }

}
