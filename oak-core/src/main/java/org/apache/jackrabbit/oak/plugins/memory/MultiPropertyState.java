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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.*;

/**
 * Multi-valued property state.
 */
public class MultiPropertyState extends EmptyPropertyState {

    private final List<CoreValue> values;

    public MultiPropertyState(String name, List<CoreValue> values) {
        super(name, getBaseType(values));
        this.values = Collections.unmodifiableList(
                new ArrayList<CoreValue>(checkNotNull(values)));
    }

    private static Type<?> getBaseType(List<CoreValue> values) {
        if (values.isEmpty()) {
            return STRINGS;
        }
        else {
            return Type.fromTag(values.get(0).getType(), true);
        }
    }

    @Override
    @Nonnull
    @Deprecated
    public List<CoreValue> getValues() {
        return values;
    }

    @Override
    public long size(int index) {
        return values.get(index).length();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(Type<T> type) {
        if (!type.isArray()) {
            throw new IllegalStateException("Not a single valued property");
        }

        switch (type.tag()) {
            case PropertyType.STRING: return (T) getStrings();
            case PropertyType.BINARY: return (T) getBinaries();
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
    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (type.isArray()) {
            throw new IllegalArgumentException("Nested arrays not supported");
        }

        switch (type.tag()) {
            case PropertyType.STRING: return (T) values.get(index).getString();
            case PropertyType.BINARY: return (T) getBlob(values.get(index));
            case PropertyType.LONG: return (T) (Long) values.get(index).getLong();
            case PropertyType.DOUBLE: return (T) (Double) values.get(index).getDouble();
            case PropertyType.DATE: return (T) values.get(index).getString();
            case PropertyType.BOOLEAN: return (T) (Boolean) values.get(index).getBoolean();
            case PropertyType.NAME: return (T) values.get(index).getString();
            case PropertyType.PATH: return (T) values.get(index).getString();
            case PropertyType.REFERENCE: return (T) values.get(index).getString();
            case PropertyType.WEAKREFERENCE: return (T) values.get(index).getString();
            case PropertyType.URI: return (T) values.get(index).getString();
            case PropertyType.DECIMAL: return (T) values.get(index).getDecimal();
            default: throw new IllegalArgumentException("Invalid type:" + type);
        }
    }

    @Override
    public long count() {
        return values.size();
    }

    private List<String> getStrings() {
        List<String> strings = new ArrayList<String>();
        for (CoreValue value: values) {
            strings.add(value.getString());
        }
        return strings;
    }

    private List<Long> getLongs() {
        List<Long> longs = new ArrayList<Long>();
        for (CoreValue value: values) {
            longs.add(value.getLong());
        }
        return longs;
    }

    private List<Double> getDoubles() {
        List<Double> doubles = new ArrayList<Double>();
        for (CoreValue value: values) {
            doubles.add(value.getDouble());
        }
        return doubles;
    }

    private List<Boolean> getBooleans() {
        List<Boolean> booleans = new ArrayList<Boolean>();
        for (CoreValue value: values) {
            booleans.add(value.getBoolean());
        }
        return booleans;
    }

    private List<BigDecimal> getDecimals() {
        List<BigDecimal> decimals = new ArrayList<BigDecimal>();
        for (CoreValue value: values) {
            decimals.add(value.getDecimal());
        }
        return decimals;
    }

    private List<Blob> getBinaries() {
        List<Blob> binaries = new ArrayList<Blob>();
        for (CoreValue value: values) {
            binaries.add(getBlob(value));
        }
        return binaries;
    }
}
