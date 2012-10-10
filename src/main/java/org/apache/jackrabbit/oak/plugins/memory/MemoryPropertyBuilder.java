/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class MemoryPropertyBuilder<T> implements PropertyBuilder<T> {
    private final Type<T> type;

    private String name;
    private List<T> values = Lists.newArrayList();

    public MemoryPropertyBuilder(Type<T> type) {
        this.type = type;
    }

    public static <T> PropertyBuilder<T> create(Type<T> type) {
        checkArgument(!type.isArray(), "type must not be array");
        return new MemoryPropertyBuilder<T>(type);
    }

    public static <T> PropertyBuilder<T> create(Type<T> type, PropertyState property) {
        checkArgument(!type.isArray(), "type must not be array");
        return new MemoryPropertyBuilder<T>(type)
            .assignFrom(property);
    }

    public static <T> PropertyBuilder<T> create(Type<T> type, String name) {
        checkArgument(!type.isArray(), "type must not be array");
        return new MemoryPropertyBuilder<T>(type)
            .setName(name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public T getValue() {
        return values.isEmpty() ? null : values.get(0);
    }

    @Nonnull
    @Override
    public List<T> getValues() {
        return Lists.newArrayList(values);
    }

    @Override
    public T getValue(int index) {
        return values.get(index);
    }

    @Override
    public boolean hasValue(Object value) {
        return values.contains(value);
    }

    @Override
    public int count() {
        return values.size();
    }

    @Override
    public boolean isArray() {
        return count() != 1;
    }

    @Override
    public boolean isEmpty() {
        return count() == 0;
    }

    @Nonnull
    @Override
    public PropertyState getPropertyState() {
        return getPropertyState(false);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public PropertyState getPropertyState(boolean asArray) {
        checkState(name != null, "Property has no name");
        if (values.isEmpty()) {
            return PropertyStates.emptyProperty(name, type);
        }
        else if (isArray() || asArray) {
            switch (type.tag()) {
                case PropertyType.STRING:
                    return PropertyStates.stringProperty(name, (Iterable<String>) values);
                case PropertyType.BINARY:
                    return PropertyStates.binaryPropertyFromBlob(name, (Iterable<Blob>) values);
                case PropertyType.LONG:
                    return PropertyStates.longProperty(name, (Iterable<Long>) values);
                case PropertyType.DOUBLE:
                    return PropertyStates.doubleProperty(name, (Iterable<Double>) values);
                case PropertyType.BOOLEAN:
                    return PropertyStates.booleanProperty(name, (Iterable<Boolean>) values);
                case PropertyType.DECIMAL:
                    return PropertyStates.decimalProperty(name, (Iterable<BigDecimal>) values);
                default:
                    return new GenericsPropertyState(name, (List<String>) Lists.newArrayList(values),
                            Type.fromTag(type.tag(), true));
            }
        }
        else {
            T value = values.get(0);
            switch (type.tag()) {
                case PropertyType.STRING:
                    return PropertyStates.stringProperty(name, (String) value);
                case PropertyType.BINARY:
                    return PropertyStates.binaryProperty(name, (Blob) value);
                case PropertyType.LONG:
                    return PropertyStates.longProperty(name, (Long) value);
                case PropertyType.DOUBLE:
                    return PropertyStates.doubleProperty(name, (Double) value);
                case PropertyType.BOOLEAN:
                    return PropertyStates.booleanProperty(name, (Boolean) value);
                case PropertyType.DECIMAL:
                    return PropertyStates.decimalProperty(name, (BigDecimal) value);
                default:
                    return new GenericPropertyState(name, (String) value, type);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public PropertyBuilder<T> assignFrom(PropertyState property) {
        if (property != null) {
            setName(property.getName());
            if (property.isArray()) {
                setValues((Iterable<T>) property.getValue(type.getArrayType()));
            }
            else {
                setValue(property.getValue(type));
            }
        }
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> setName(String name) {
        this.name = name;
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> setValue(T value) {
        values.clear();
        values.add(value);
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> addValue(T value) {
        values.add(value);
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> setValue(T value, int index) {
        values.set(index, value);
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> setValues(Iterable<T> values) {
        this.values = Lists.newArrayList(values);
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> removeValue(int index) {
        values.remove(index);
        return this;
    }

    @Nonnull
    @Override
    public PropertyBuilder<T> removeValue(Object value) {
        values.remove(value);
        return this;
    }
}
