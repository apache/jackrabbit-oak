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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Property state that contains an empty array of values. Used as a base
 * class for {@link SinglePropertyState} and {@link MultiPropertyState}.
 */
class EmptyPropertyState implements PropertyState {

    private final String name;
    private final Type<?> type;

    public EmptyPropertyState(String name, Type<?> type) {
        this.name = checkNotNull(name);
        this.type = type;
    }

    protected static Blob getBlob(CoreValue value) {
        return new BlobImpl(value);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isArray() {
        return true;
    }

    @Override
    @Nonnull
    @Deprecated
    public CoreValue getValue() {
        throw new IllegalStateException("Not a single valued property");
    }

    @Override
    @Nonnull
    @Deprecated
    public List<CoreValue> getValues() {
        return Collections.emptyList();
    }

    @Override
    public Type<?> getType() {
        return type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            return (T) Collections.emptyList();
        }
        else {
            throw new IllegalStateException("Not a single valued property");
        }
    }

    @Override
    public <T> T getValue(Type<T> type, int index) {
        throw new IndexOutOfBoundsException(String.valueOf(index));
    }

    @Override
    public long size() {
        throw new IllegalStateException("Not a single valued property");
    }

    @Override
    public long size(int index) {
        throw new IndexOutOfBoundsException(String.valueOf(index));
    }

    @Override
    public long count() {
        return 0;
    }

    //------------------------------------------------------------< Object >--

    /**
     * Checks whether the given object is equal to this one. Two property
     * states are considered equal if both their names and encoded values
     * match. Subclasses may override this method with a more efficient
     * equality check if one is available.
     *
     * @param that target of the comparison
     * @return {@code true} if the objects are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof PropertyState) {
            PropertyState other = (PropertyState) that;
            return getName().equals(other.getName())
                    && isArray() == other.isArray()
                    && getValues().equals(other.getValues());
        } else {
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
        return getName() + '=' + getValues();
    }

}
