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

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;

/**
 * Abstract base class for single valued {@code PropertyState} implementations.
 */
abstract class SinglePropertyState extends EmptyPropertyState {

    /**
     * Create a new property state with the given {@code name}
     * @param name  The name of the property state.
     */
    protected SinglePropertyState(String name) {
        super(name);
    }

    /**
     * String representation of the value of the property state.
     * @return
     */
    protected abstract String getString();

    /**
     * @return  A {@link StringBasedBlob} instance created by calling
     * {@link #getString()}.
     */
    protected Blob getBlob() {
        return Conversions.convert(getString()).toBinary();
    }

    /**
     * @return  {@code getLong(getString())}
     */
    protected long getLong() {
        return Conversions.convert(getString()).toLong();
    }

    /**
     * @return  {@code getDouble(getString())}
     */
    protected double getDouble() {
        return Conversions.convert(getString()).toDouble();
    }

    /**
     * @return  {@code StringPropertyState.getBoolean(getString())}
     */
    protected boolean getBoolean() {
        return Conversions.convert(getString()).toBoolean();
    }

    /**
     * @return  {@code getDecimal(getString())}
     */
    protected BigDecimal getDecimal() {
        return Conversions.convert(getString()).toDecimal();
    }

    /**
     * @return  {@code false}
     */
    @Override
    public boolean isArray() {
        return false;
    }

    /**
     * @throws IllegalArgumentException if {@code type} is not one of the
     * values defined in {@link Type}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            switch (type.tag()) {
                case PropertyType.STRING: return (T) singleton(getString());
                case PropertyType.BINARY: return (T) singleton(getBlob());
                case PropertyType.LONG: return (T) singleton(getLong());
                case PropertyType.DOUBLE: return (T) singleton(getDouble());
                case PropertyType.DATE: return (T) singleton(getString());
                case PropertyType.BOOLEAN: return (T) singleton(getBoolean());
                case PropertyType.NAME: return (T) singleton(getString());
                case PropertyType.PATH: return (T) singleton(getString());
                case PropertyType.REFERENCE: return (T) singleton(getString());
                case PropertyType.WEAKREFERENCE: return (T) singleton(getString());
                case PropertyType.URI: return (T) singleton(getString());
                case PropertyType.DECIMAL: return (T) singleton(getDecimal());
                default: throw new IllegalArgumentException("Invalid primitive type:" + type);
            }
        }
        else {
            switch (type.tag()) {
                case PropertyType.STRING: return (T) getString();
                case PropertyType.BINARY: return (T) getBlob();
                case PropertyType.LONG: return (T) (Long) getLong();
                case PropertyType.DOUBLE: return (T) (Double) getDouble();
                case PropertyType.DATE: return (T) getString();
                case PropertyType.BOOLEAN: return (T) (Boolean) getBoolean();
                case PropertyType.NAME: return (T) getString();
                case PropertyType.PATH: return (T) getString();
                case PropertyType.REFERENCE: return (T) getString();
                case PropertyType.WEAKREFERENCE: return (T) getString();
                case PropertyType.URI: return (T) getString();
                case PropertyType.DECIMAL: return (T) getDecimal();
                default: throw new IllegalArgumentException("Invalid array type:" + type);
            }
        }
    }

    /**
     * @throws IllegalArgumentException  if {@code type.isArray} is {@code true}
     * @throws IndexOutOfBoundsException  if {@code index != 0}
     */
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
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
        return getString().length();
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
