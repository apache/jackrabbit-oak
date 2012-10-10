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
package org.apache.jackrabbit.oak.spi.state;

import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Builder interface for constructing new {@link PropertyState node states}.
 */
public interface PropertyBuilder<T> {

    /**
     * @return The name of the property state
     */
    @CheckForNull
    String getName();

    /**
     * @return The value of the property state or {@code null} if {@code isEmpty} is {@code true}
     */
    @CheckForNull
    T getValue();

    /**
     * @return  A list of values of the property state
     */
    @Nonnull
    List<T> getValues();

    /**
     * @param index
     * @return  The value of the property state at the given {@code index}.
     * @throws IndexOutOfBoundsException  if {@code index >= count}
     */
    @Nonnull
    T getValue(int index);

    /**
     * @param value
     * @return  {@code true} iff the property state contains {@code value}.
     */
    boolean hasValue(Object value);

    /**
     * @return  The number of values of the property state
     */
    int count();

    /**
     * @return  {@code true} iff {@code count() != 1}
     */
    boolean isArray();

    /**
     * @return  {{@code true}} iff {@code count() == 0}
     * @return
     */
    boolean isEmpty();

    /**
     * Returns an immutable property state that matches the current state of
     * the builder. The {@code asArray} flag can be used to coerce a property
     * state with a single value into a multi valued property state.
     * Equivalent to {@code getPropertyState(false)}
     *
     * @return immutable property state
     * @throws IllegalStateException  If the name of the property is not set
     */
    @Nonnull
    PropertyState getPropertyState();

    /**
     * Returns an immutable property state that matches the current state of
     * the builder. The {@code asArray} flag can be used to coerce a property
     * state with a single value into a multi valued property state.
     *
     * @param asArray  If {@code true} the builder creates a multi valued property state
     * @return immutable property state
     * @throws IllegalStateException  If the name of the property is not set
     */
    @Nonnull
    PropertyState getPropertyState(boolean asArray);

    /**
     * Clone {@code property} to the property state being built. After
     * this call {@code getPropertyState(property.isArray()).equals(property)} will hold.
     * @param property  the property to clone
     * @return  {@code this}
     */
    @Nonnull
    PropertyBuilder<T> assignFrom(PropertyState property);

    /**
     * Set the name of the property
     * @param name
     * @return  {@code this}
     */
    @Nonnull
    PropertyBuilder<T> setName(String name);

    /**
     * Set the value of the property state clearing all previously set values.
     * @param value  value to set
     * @return  {@code this}
     */
    @Nonnull
    PropertyBuilder<T> setValue(T value);

    /**
     * Add a value to the end of the list of values of the property state.
     * @param value  value to add
     * @return  {@code this}
     */
    @Nonnull
    PropertyBuilder<T> addValue(T value);

    /**
     * Set the value of the property state at the given {@code index}.
     * @param value  value to set
     * @param index  index to set the value
     * @return  {@code this}
     * @throws IndexOutOfBoundsException  if {@code index >= count}
     */
    @Nonnull
    PropertyBuilder<T> setValue(T value, int index);

    /**
     * Set the values of the property state clearing all previously set values.
     * @param values
     * @return  {@code this}
     */
    @Nonnull
    PropertyBuilder<T> setValues(Iterable<T> values);

    /**
     * Remove the value at the given {@code index}
     * @param index
     * @return  {@code this}
     * @throws IndexOutOfBoundsException  if {@code index >= count}
     */
    @Nonnull
    PropertyBuilder<T> removeValue(int index);

    /**
     * Remove the given value from the property state
     * @param value  value to remove
     * @return  {@code this}
     */
    @Nonnull
    PropertyBuilder<T> removeValue(Object value);

}
