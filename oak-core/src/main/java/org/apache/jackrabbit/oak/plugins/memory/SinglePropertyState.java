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

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Single-valued property state.
 */
public class SinglePropertyState extends EmptyPropertyState {

    public static PropertyState create(String name, boolean value) {
        return new SinglePropertyState(name, BooleanValue.create(value));
    }

    public static PropertyState create(String name, double value) {
        return new SinglePropertyState(name, new DoubleValue(value));
    }

    public static PropertyState create(String name, long value) {
        return new SinglePropertyState(name, new LongValue(value));
    }

    public static PropertyState create(String name, String value) {
        return new SinglePropertyState(name, new StringValue(value));
    }

    private final CoreValue value;

    public SinglePropertyState(String name, CoreValue value) {
        super(name, Type.fromTag(value.getType(), false));
        this.value = checkNotNull(value);
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    @Nonnull
    @Deprecated
    public CoreValue getValue() {
        return value;
    }

    @Override
    @Nonnull
    @Deprecated
    public List<CoreValue> getValues() {
        return Collections.singletonList(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            switch (type.tag()) {
                case PropertyType.STRING: return (T) Collections.singleton(value.getString());
                case PropertyType.BINARY: return (T) Collections.singleton(getBlob(value));
                case PropertyType.LONG: return (T) Collections.singleton(value.getLong());
                case PropertyType.DOUBLE: return (T) Collections.singleton(value.getDouble());
                case PropertyType.DATE: return (T) Collections.singleton(value.getString());
                case PropertyType.BOOLEAN: return (T) Collections.singleton(value.getBoolean());
                case PropertyType.NAME: return (T) Collections.singleton(value.getString());
                case PropertyType.PATH: return (T) Collections.singleton(value.getString());
                case PropertyType.REFERENCE: return (T) Collections.singleton(value.getString());
                case PropertyType.WEAKREFERENCE: return (T) Collections.singleton(value.getString());
                case PropertyType.URI: return (T) Collections.singleton(value.getString());
                case PropertyType.DECIMAL: return (T) Collections.singleton(value.getDecimal());
                default: throw new IllegalArgumentException("Invalid primitive type:" + type);
            }
        }
        else {
            switch (type.tag()) {
                case PropertyType.STRING: return (T) value.getString();
                case PropertyType.BINARY: return (T) getBlob(value);
                case PropertyType.LONG: return (T) (Long) value.getLong();
                case PropertyType.DOUBLE: return (T) (Double) value.getDouble();
                case PropertyType.DATE: return (T) value.getString();
                case PropertyType.BOOLEAN: return (T) (Boolean) value.getBoolean();
                case PropertyType.NAME: return (T) value.getString();
                case PropertyType.PATH: return (T) value.getString();
                case PropertyType.REFERENCE: return (T) value.getString();
                case PropertyType.WEAKREFERENCE: return (T) value.getString();
                case PropertyType.URI: return (T) value.getString();
                case PropertyType.DECIMAL: return (T) value.getDecimal();
                default: throw new IllegalArgumentException("Invalid array type:" + type);
            }
        }
    }

    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (type.isArray()) {
            throw new IllegalArgumentException("Nested arrows not supported");
        }
        if (index != 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        return getValue(type);
    }

    @Override
    public long size() {
        return value.length();
    }

    @Override
    public long size(int index) {
        if (index != 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return size();
    }

    @Override
    public int count() {
        return 1;
    }
}
