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
import javax.jcr.PropertyType;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * Abstract base class for {@link PropertyState} implementations
 * providing default implementation which correspond to a property
 * without any value.
 */
public abstract class EmptyPropertyState implements PropertyState {
    private final String name;

    /**
     * Create a new property state with the given {@code name}
     * @param name  The name of the property state.
     */
    protected EmptyPropertyState(String name) {
        this.name = name;
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

    //------------------------------------------------------------< Object >--

    /**
     * Checks whether the given object is equal to this one. Two property
     * states are considered equal if their names and types match and
     * their string representation of their values are equal.
     * Subclasses may override this method with a more efficient
     * equality check if one is available.
     *
     * @param other target of the comparison
     * @return {@code true} if the objects are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        else if (other instanceof PropertyState) {
            PropertyState that = (PropertyState) other;
            if (!getName().equals(that.getName())) {
                return false;
            }
            if (!getType().equals(that.getType())) {
                return false;
            }
            if (getType().tag() == PropertyType.BINARY) {
                return Iterables.elementsEqual(
                        getValue(BINARIES), that.getValue(BINARIES));
            }
            else {
                return Iterables.elementsEqual(
                        getValue(STRINGS), that.getValue(STRINGS));
            }
        }
        else {
            return false;
        }
    }

    /**
     * Returns a hash code that's compatible with how the
     * {@link #equals(Object)} method is implemented. The current
     * implementation simply returns the hash code of the property name
     * since {@link PropertyState} instances are not intended for use as
     * hash keys.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        if (isArray()) {
            return getName() + '=' + getValue(STRINGS);
        }
        else {
            return getName() + '=' + getValue(STRING);
        }
    }

}
