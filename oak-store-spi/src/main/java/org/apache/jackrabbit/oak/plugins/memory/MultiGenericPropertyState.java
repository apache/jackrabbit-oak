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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.api.Type.DATES;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.PATHS;
import static org.apache.jackrabbit.oak.api.Type.REFERENCES;
import static org.apache.jackrabbit.oak.api.Type.URIS;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCES;

public class MultiGenericPropertyState extends MultiPropertyState<String> {
    private final Type<?> type;

    /**
     * @throws IllegalArgumentException if {@code type.isArray()} is {@code false}
     */
    public MultiGenericPropertyState(String name, Iterable<String> values, Type<?> type) {
        super(name, values);
        checkArgument(type.isArray());
        this.type = type;
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of dates.
     *
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#DATES}
     */
    public static PropertyState dateProperty(String name, Iterable<String> values) {
        return new MultiGenericPropertyState(name, values, DATES);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of names.
     * No validation is performed on the strings passed for {@code values}.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#NAMES}
     */
    public static PropertyState nameProperty(String name, Iterable<String> values) {
        return new MultiGenericPropertyState(name, values, NAMES);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of paths.
     * No validation is performed on the strings passed for {@code values}.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#PATHS}
     */
    public static PropertyState pathProperty(String name, Iterable<String> values) {
        return new MultiGenericPropertyState(name, values, PATHS);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of references.
     * No validation is performed on the strings passed for {@code values}.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#REFERENCES}
     */
    public static PropertyState referenceProperty(String name, Iterable<String> values) {
        return new MultiGenericPropertyState(name, values, REFERENCES);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of weak references.
     * No validation is performed on the strings passed for {@code values}.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#WEAKREFERENCES}
     */
    public static PropertyState weakreferenceProperty(String name, Iterable<String> values) {
        return new MultiGenericPropertyState(name, values, WEAKREFERENCES);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of URIs.
     * No validation is performed on the strings passed for {@code values}.
     * @param name  The name of the property state
     * @param values  The values of the property state
     * @return  The new property state of type {@link Type#URIS}
     */
    public static PropertyState uriProperty(String name, Iterable<String> values) {
        return new MultiGenericPropertyState(name, values, URIS);
    }

    @Override
    public Converter getConverter(String value) {
        return Conversions.convert(value, type.getBaseType());
    }

    @Override
    public Type<?> getType() {
        return type;
    }
}
