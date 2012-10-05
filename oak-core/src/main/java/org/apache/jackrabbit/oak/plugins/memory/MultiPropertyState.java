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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;

abstract class MultiPropertyState extends EmptyPropertyState {
    protected MultiPropertyState(String name) {
        super(name);
    }

    protected abstract Iterable<String> getStrings();
    protected abstract String getString(int index);

    protected Iterable<Blob> getBlobs() {
        return Iterables.transform(getStrings(), new Function<String, Blob>() {
            @Override
            public Blob apply(String value) {
                return new StringBasedBlob(value);
            }
        });
    }

    protected Iterable<Long> getLongs() {
        return Iterables.transform(getStrings(), new Function<String, Long>() {
            @Override
            public Long apply(String value) {
                return Long.parseLong(value);
            }
        });
    }

    protected Iterable<Double> getDoubles() {
        return Iterables.transform(getStrings(), new Function<String, Double>() {
            @Override
            public Double apply(String value) {
                return Double.parseDouble(value);
            }
        });
    }

    protected Iterable<Boolean> getBooleans() {
        throw new UnsupportedOperationException("Unsupported conversion.");
    }

    protected Iterable<BigDecimal> getDecimals() {
        return Iterables.transform(getStrings(), new Function<String, BigDecimal>() {
            @Override
            public BigDecimal apply(String value) {
                return new BigDecimal(value);
            }
        });
    }

    protected Blob getBlob(int index) {
        return new StringBasedBlob(getString(index));
    }

    protected long getLong(int index) {
        return Long.parseLong(getString(index));
    }

    protected double getDouble(int index) {
        return Double.parseDouble(getString(index));
    }

    protected boolean getBoolean(int index) {
        throw new UnsupportedOperationException("Unsupported conversion.");
    }

    protected BigDecimal getDecimal(int index) {
        return new BigDecimal(getString(index));
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        if (!type.isArray()) {
            throw new IllegalStateException("Not a single valued property");
        }

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

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (type.isArray()) {
            throw new IllegalArgumentException("Nested arrays not supported");
        }
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
    public long size(int index) {
        return getString(index).length();
    }

}
