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
package org.apache.jackrabbit.oak.api;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import javax.jcr.PropertyType;

import com.google.common.collect.Iterables;

/**
 * Abstract base class for {@link PropertyState} implementations. This
 * class provides default implementations of basic {@link Object} methods,
 * for consistency across all property states.
 */
public abstract class AbstractPropertyState implements PropertyState {

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
        } else if (other instanceof PropertyState) {
            PropertyState that = (PropertyState) other;
            if (!getName().equals(that.getName())) {
                return false;
            } else if (!getType().equals(that.getType())) {
                return false;
            } else if (getType().tag() == PropertyType.BINARY) {
                return Iterables.elementsEqual(
                        getValue(BINARIES), that.getValue(BINARIES));
            } else {
                return Iterables.elementsEqual(
                        getValue(STRINGS), that.getValue(STRINGS));
            }
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
        return getName().hashCode();
    }

    @Override
    public String toString() {
        if (getType() == Type.BINARIES) {
            return getName() + " = [" + count() + " binaries]"; 
        } else if (getType() == Type.BINARY) {
            return getName() + " = {" + size() + " bytes}"; 
        } else if (isArray()) {
            return getName() + " = " + getValue(STRINGS);
        } else {
            return getName() + " = " + getValue(STRING);
        }
    }

}
