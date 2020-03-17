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

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.BinaryBasedBlob;
import org.apache.jackrabbit.oak.plugins.value.OakValue;

/**
 * Abstract base class for {@link org.apache.jackrabbit.oak.api.PropertyState} implementations. This
 * class provides default implementations of basic {@link Object} methods,
 * for consistency across all property states.
 */
public abstract class AbstractPropertyState implements PropertyState {

    /**
     * Checks whether the given two property states are equal. They are
     * considered equal if their names and types match, they have an equal
     * number of values, and each of the values is equal with the
     * corresponding value in the other property.
     *
     * @param a first property state
     * @param b second property state
     * @return {@code true} if the properties are equal, {@code false} otherwise
     */
    public static boolean equal(PropertyState a, PropertyState b) {
        if (Objects.equal(a.getName(), b.getName())
                && Objects.equal(a.getType(), b.getType())) {
            Type<?> type = a.getType();
            if (a.isArray()) {
                return a.count() == b.count()
                        && Iterables.elementsEqual(
                                (Iterable<?>) a.getValue(type),
                                (Iterable<?>) b.getValue(type));
            } else {
                return Objects.equal(a.getValue(type), b.getValue(type));
            }
        } else {
            return false;
        }
    }

    public static int hashCode(PropertyState property) {
        return property.getName().hashCode();
    }

    public static String toString(PropertyState property) {
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == Type.BINARIES) {
            return name + " = [" + property.count() + " binaries]";
        } else if (type == Type.BINARY) {
            return name + " = {" + getBinarySize(property) + " bytes}";
        } else {
            return name + " = " + property.getValue(type);
        }
    }

    /**
     * Checks whether the given object is equal to this one. See the
     * {@link #equal(PropertyState, PropertyState)} method for the definition
     * of property state equality. Subclasses may override this method with
     * a more efficient equality check if one is available.
     *
     * @param other target of the comparison
     * @return {@code true} if the objects are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else {
            return other instanceof PropertyState
                    && equal(this, (PropertyState) other);
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
        return hashCode(this);
    }

    @Override
    public String toString() {
        return toString(this);
    }

    //~-------------------------------------------< internal >

    private static long getBinarySize(PropertyState property) {
        try {
            return property.size();
        } catch (Exception e) {
            return -1;
        }
    }

    static Blob getBlob(Value value) throws RepositoryException {
        if (value instanceof OakValue) {
            return ((OakValue) value).getBlob();
        } else {
            return new BinaryBasedBlob(value.getBinary());
        }
    }

}
