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
import static java.util.Collections.singleton;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

/**
 * Abstract base class for single valued {@code PropertyState} implementations.
 */
abstract class SinglePropertyState<T> extends EmptyPropertyState {

    /**
     * Create a new property state with the given {@code name}
     * @param name  The name of the property state.
     */
    protected SinglePropertyState(@Nonnull String name) {
        super(name);
    }

    /**
     * @return  {@code false}
     */
    @Override
    public boolean isArray() {
        return false;
    }

    /**
     * Create a converter for converting the value of this property to other types.
     * @return  A converter for the value of this property
     */
    public abstract Converter getConverter();

    @SuppressWarnings("unchecked")
    private <S> S  convertTo(Type<S> type) {
        switch (type.tag()) {
            case PropertyType.STRING: return (S) getConverter().toString();
            case PropertyType.BINARY: return (S) getConverter().toBinary();
            case PropertyType.LONG: return (S) (Long) getConverter().toLong();
            case PropertyType.DOUBLE: return (S) (Double) getConverter().toDouble();
            case PropertyType.DATE: return (S) getConverter().toDate();
            case PropertyType.BOOLEAN: return (S) (Boolean) getConverter().toBoolean();
            case PropertyType.NAME: return (S) getConverter().toString();
            case PropertyType.PATH: return (S) getConverter().toString();
            case PropertyType.REFERENCE: return (S) getConverter().toString();
            case PropertyType.WEAKREFERENCE: return (S) getConverter().toString();
            case PropertyType.URI: return (S) getConverter().toString();
            case PropertyType.DECIMAL: return (S) getConverter().toDecimal();
            default: throw new IllegalArgumentException("Unknown type:" + type);
        }
    }

    /**
     * The value of this property
     * @return  Value of this property
     */
    public abstract T getValue();

    /**
     * @throws IllegalArgumentException if {@code type} is not one of the
     * values defined in {@link Type}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <S> S getValue(Type<S> type) {
        if (type.isArray()) {
            if (getType() == type.getBaseType()) {
                return (S) singleton(getValue());
            } else {
                return (S) singleton(convertTo(type.getBaseType()));
            }
        } else {
            if (getType() == type) {
                return (S) getValue();
            } else {
                return convertTo(type);
            }
        }
    }

    /**
     * @throws IllegalArgumentException  if {@code type.isArray} is {@code true}
     * @throws IndexOutOfBoundsException  if {@code index != 0}
     */
    @Nonnull
    @Override
    public <S> S getValue(Type<S> type, int index) {
        checkArgument(!type.isArray(), "Type must not be an array type");
        if (index != 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return getValue(type);
    }

    /**
     * @return  {@code getString().length()}
     */
    @Override
    public long size() {
        return convertTo(Type.STRING).length();
    }

    /**
     * @return  {@code size}
     * @throws IndexOutOfBoundsException  if {@code index != 0}
     */
    @Override
    public long size(int index) {
        if (index != 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return size();
    }

    /**
     * @return {@code 1}
     */
    @Override
    public int count() {
        return 1;
    }
}
