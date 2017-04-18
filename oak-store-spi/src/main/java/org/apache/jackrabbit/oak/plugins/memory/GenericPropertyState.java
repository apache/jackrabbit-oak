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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.PATH;
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.URI;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;

public class GenericPropertyState extends SinglePropertyState<String> {
    private final String value;
    private final Type<?> type;

    /**
     * @throws IllegalArgumentException if {@code type.isArray()} is {@code true}
     */
    public GenericPropertyState(
            @Nonnull String name, @Nonnull String value, @Nonnull Type<?> type) {
        super(name);
        this.value = checkNotNull(value);
        this.type = checkNotNull(type);
        checkArgument(!type.isArray());
    }

    /**
     * Create a {@code PropertyState} from a date. No validation is performed
     * on the string passed for {@code value}.
     *
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#DATE}
     */
    public static PropertyState dateProperty(String name, String value) {
        return new GenericPropertyState(name, value, DATE);
    }

    /**
     * Create a {@code PropertyState} from a name. No validation is performed
     * on the string passed for {@code value}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#NAME}
     */
    public static PropertyState nameProperty(String name, String value) {
        return new GenericPropertyState(name, value, NAME);
    }

    /**
     * Create a {@code PropertyState} from a path. No validation is performed
     * on the string passed for {@code value}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#PATH}
     */
    public static PropertyState pathProperty(String name, String value) {
        return new GenericPropertyState(name, value, PATH);
    }

    /**
     * Create a {@code PropertyState} from a reference. No validation is performed
     * on the string passed for {@code value}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#REFERENCE}
     */
    public static PropertyState referenceProperty(String name, String value) {
        return new GenericPropertyState(name, value, REFERENCE);
    }

    /**
     * Create a {@code PropertyState} from a weak reference. No validation is performed
     * on the string passed for {@code value}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#WEAKREFERENCE}
     */
    public static PropertyState weakreferenceProperty(String name, String value) {
        return new GenericPropertyState(name, value, WEAKREFERENCE);
    }

    /**
     * Create a {@code PropertyState} from a URI. No validation is performed
     * on the string passed for {@code value}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#URI}
     */
    public static PropertyState uriProperty(String name, String value) {
        return new GenericPropertyState(name, value, URI);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Converter getConverter() {
        return Conversions.convert(value, type);
    }

    @Override
    public Type<?> getType() {
        return type;
    }
}
