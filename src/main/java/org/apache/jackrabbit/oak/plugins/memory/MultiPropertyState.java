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

import static com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

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
    protected MultiPropertyState(String name, Iterable<T> values) {
        super(name);
        this.values = Lists.newArrayList(values);
    }

    /**
     * Create a converter for converting a value to other types.
     * @param  value  The value to convert
     * @return  A converter for the value of this property
     */
    public abstract Converter getConverter(T value);

    @SuppressWarnings("unchecked")
    private <S> S  convertTo(Type<S> type) {
        switch (type.tag()) {
            case PropertyType.STRING:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toString();
                    }
                });
            case PropertyType.BINARY:
                return (S) Iterables.transform(values, new Function<T, Blob>() {
                    @Override
                    public Blob apply(T value) {
                        return getConverter(value).toBinary();
                    }
                });
            case PropertyType.LONG:
                return (S) Iterables.transform(values, new Function<T, Long>() {
                    @Override
                    public Long apply(T value) {
                        return getConverter(value).toLong();
                    }
                });
            case PropertyType.DOUBLE:
                return (S) Iterables.transform(values, new Function<T, Double>() {
                    @Override
                    public Double apply(T value) {
                        return getConverter(value).toDouble();
                    }
                });
            case PropertyType.DATE:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toDate();
                    }
                });
            case PropertyType.BOOLEAN:
                return (S) Iterables.transform(values, new Function<T, Boolean>() {
                    @Override
                    public Boolean apply(T value) {
                        return getConverter(value).toBoolean();
                    }
                });
            case PropertyType.NAME:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toString();
                    }
                });
            case PropertyType.PATH:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toString();
                    }
                });
            case PropertyType.REFERENCE:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toString();
                    }
                });
            case PropertyType.WEAKREFERENCE:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toString();
                    }
                });
            case PropertyType.URI:
                return (S) Iterables.transform(values, new Function<T, String>() {
                    @Override
                    public String apply(T value) {
                        return getConverter(value).toString();
                    }
                });
            case PropertyType.DECIMAL:
                return (S) Iterables.transform(values, new Function<T, BigDecimal>() {
                    @Override
                    public BigDecimal apply(T value) {
                        return getConverter(value).toDecimal();
                    }
                });
            default: throw new IllegalArgumentException("Unknown type:" + type);
        }
    }

    /**
     * @throws IllegalArgumentException if {@code type} is not one of the
     * values defined in {@link Type} or if {@code type.isArray()} is {@code false}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <S> S getValue(Type<S> type) {
        checkArgument(type.isArray(), "Type must be an array type");
        if (getType() == type) {
            return (S) values;
        }
        else {
            return convertTo(type);
        }
    }

    @SuppressWarnings("unchecked")
    private <S> S  convertTo(Type<S> type, int index) {
        switch (type.tag()) {
            case PropertyType.STRING: return (S) getConverter(values.get(index)).toString();
            case PropertyType.BINARY: return (S) getConverter(values.get(index)).toBinary();
            case PropertyType.LONG: return (S) (Long) getConverter(values.get(index)).toLong();
            case PropertyType.DOUBLE: return (S) (Double) getConverter(values.get(index)).toDouble();
            case PropertyType.DATE: return (S) getConverter(values.get(index)).toString();
            case PropertyType.BOOLEAN: return (S) (Boolean) getConverter(values.get(index)).toBoolean();
            case PropertyType.NAME: return (S) getConverter(values.get(index)).toString();
            case PropertyType.PATH: return (S) getConverter(values.get(index)).toString();
            case PropertyType.REFERENCE: return (S) getConverter(values.get(index)).toString();
            case PropertyType.WEAKREFERENCE: return (S) getConverter(values.get(index)).toString();
            case PropertyType.URI: return (S) getConverter(values.get(index)).toString();
            case PropertyType.DECIMAL: return (S) getConverter(values.get(index)).toDecimal();
            default: throw new IllegalArgumentException("Unknown type:" + type);
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
    public <S> S getValue(Type<S> type, int index) {
        checkArgument(!type.isArray(), "Type must not be an array type");
        if (getType().getBaseType() == type) {
            return (S) values.get(index);
        }
        else {
            return convertTo(type, index);
        }
    }

    @Override
    public final int count() {
        return values.size();
    }

    @Override
    public long size(int index) {
        return convertTo(Type.STRING, index).length();
    }

}
