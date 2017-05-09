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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract base class for {@link PropertyState} implementations
 * providing default implementation which correspond to a property
 * without any value.
 */
public abstract class EmptyPropertyState extends AbstractPropertyState {

    private final String name;

    /**
     * Create a new property state with the given {@code name}
     * @param name  The name of the property state.
     */
    protected EmptyPropertyState(@Nonnull String name) {
        this.name = checkNotNull(name);
    }

    /**
     * Create an empty {@code PropertyState}
     * @param name  The name of the property state
     * @param type  The type of the property state
     * @return  The new property state
     */
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

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    /**
     * @return {@code true}
     */
    @Override
    public boolean isArray() {
        return true;
    }

    /**
     * @return An empty list if {@code type.isArray()} is {@code true}.
     * @throws IllegalArgumentException {@code type.isArray()} is {@code false}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        checkArgument(type.isArray(), "Type must be an array type");
        return (T) Collections.emptyList();
    }

    /**
     * @throws IndexOutOfBoundsException always
     */
    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        throw new IndexOutOfBoundsException(String.valueOf(index));
    }

    /**
     * @throws IllegalStateException always
     */
    @Override
    public long size() {
        throw new IllegalStateException("Not a single valued property");
    }

    /**
     * @throws IndexOutOfBoundsException always
     */
    @Override
    public long size(int index) {
        throw new IndexOutOfBoundsException(String.valueOf(index));
    }

    /**
     * @return {@code 0}
     */
    @Override
    public int count() {
        return 0;
    }

}
