/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.nodetype;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

/**
 * Abstract base class for the various kinds of type definition classes
 * in this package.
 */
abstract class AbstractTypeDefinition {

    private static final String[] NO_STRINGS = new String[0];

    protected final Tree definition;

    protected final NamePathMapper mapper;

    protected AbstractTypeDefinition(Tree definition, NamePathMapper mapper) {
        this.definition = checkNotNull(definition);
        this.mapper = checkNotNull(mapper);
    }

    /**
     * Returns the boolean value of the named property.
     *
     * @param name property name
     * @return property value, or {@code false} if the property does not exist
     */
    protected boolean getBoolean(@Nonnull String name) {
        PropertyState property = definition.getProperty(checkNotNull(name));
        return property != null && property.getValue(Type.BOOLEAN);
    }

    /**
     * Returns the string value of the named property.
     *
     * @param oakName property name
     * @return property value, or {@code null} if the property does not exist
     */
    @CheckForNull
    protected String getString(@Nonnull String oakName) {
        return getValue(oakName, Type.STRING);
    }

    /**
     * Returns the string values of the named property.
     *
     * @param oakName property name
     * @return property values, or {@code null} if the property does not exist
     */
    @CheckForNull
    protected String[] getStrings(@Nonnull String oakName) {
        return getValues(oakName, Type.STRING);
    }
    /**
     * Returns the name value of the named property.
     *
     * @param oakName property name
     * @return property value, or {@code null} if the property does not exist
     */
    @CheckForNull
    protected String getName(@Nonnull String oakName) {
        return getValue(oakName, Type.NAME);
    }

    /**
     * Returns the name values of the named property.
     *
     * @param oakName property name
     * @return property values, or {@code null} if the property does not exist
     */
    @CheckForNull
    protected String[] getNames(@Nonnull String oakName) {
        return getValues(oakName, Type.NAME);
    }

    private String getValue(String oakName, Type<String> type) {
        PropertyState property = definition.getProperty(checkNotNull(oakName));
        if (property != null) {
            return property.getValue(type);
        } else {
            return null;
        }
    }

    private String[] getValues(String oakName, Type<String> type) {
        String[] values = null;

        PropertyState property = definition.getProperty(checkNotNull(oakName));
        if (property != null) {
            int n = property.count();
            if (n > 0) {
                values = new String[n];
                for (int i = 0; i < n; i++) {
                    values[i] = property.getValue(type, i);
                }
            } else {
                values = NO_STRINGS;
            }
        }

        return values;
    }

}
