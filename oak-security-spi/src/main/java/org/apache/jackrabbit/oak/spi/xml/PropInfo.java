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
package org.apache.jackrabbit.oak.spi.xml;

import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

/**
 * Information about a property being imported. This class is used
 * by the XML import handlers to pass the parsed property information
 * to the import process.
 * <p>
 * In addition to carrying the actual property data, instances of this
 * class also know how to apply that data when imported either to a
 * {@link javax.jcr.Node} instance through a session or directly to a
 * {@link org.apache.jackrabbit.oak.api.Tree} instance on the oak level.
 */
public class PropInfo {

    /**
     * String of the property being imported.
     */
    private final String name;

    /**
     * Type of the property being imported.
     */
    private final int type;

    /**
     * Value(s) of the property being imported.
     */
    private final List<? extends TextValue> values;

    /**
     * Hint indicating whether the property is multi- or single-value
     */
    public enum MultipleStatus { UNKNOWN, MULTIPLE }
    private MultipleStatus multipleStatus;

    /**
     * Creates a property information instance.
     *
     * @param name name of the property being imported
     * @param type type of the property being imported
     * @param value value of the property being imported
     */
    public PropInfo(@Nullable String name, int type, @Nonnull TextValue value) {
        this(name, type, ImmutableList.of(value), MultipleStatus.UNKNOWN);
    }

    /**
     * Creates a property information instance.
     *
     * @param name name of the property being imported
     * @param type type of the property being imported
     * @param values value(s) of the property being imported
     */
    public PropInfo(@Nullable String name, int type, @Nonnull List<? extends TextValue> values) {
        this(name, type, values, ((values.size() == 1) ? MultipleStatus.UNKNOWN : MultipleStatus.MULTIPLE));
    }

    /**
     * Creates a property information instance.
     *
     * @param name name of the property being imported
     * @param type type of the property being imported
     * @param values value(s) of the property being imported
     * @param multipleStatus Hint indicating whether the property is
     */
    public PropInfo(@Nullable String name, int type,
                    @Nonnull List<? extends TextValue> values,
                    @Nonnull MultipleStatus multipleStatus) {
        this.name = name;
        this.type = type;
        this.values = ImmutableList.copyOf(values);
        this.multipleStatus = multipleStatus;
    }

    /**
     * Disposes all values contained in this property.
     */
    public void dispose() {
        for (TextValue value : values) {
            value.dispose();
        }
    }

    public int getTargetType(PropertyDefinition def) {
        int target = def.getRequiredType();
        if (target != PropertyType.UNDEFINED) {
            return target;
        } else if (type != PropertyType.UNDEFINED) {
            return type;
        } else {
            return PropertyType.STRING;
        }
    }

    @CheckForNull
    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public boolean isUnknownMultiple() {
        return multipleStatus == MultipleStatus.UNKNOWN;
    }

    @Nonnull
    public TextValue getTextValue() throws RepositoryException {
        if (multipleStatus == MultipleStatus.MULTIPLE) {
            throw new RepositoryException("Multiple import values with single-valued property definition");
        }
        return values.get(0);
    }

    @Nonnull
    public List<? extends TextValue> getTextValues() {
        return values;
    }

    @Nonnull
    public Value getValue(int targetType) throws RepositoryException {
        if (multipleStatus == MultipleStatus.MULTIPLE) {
            throw new RepositoryException("Multiple import values with single-valued property definition");
        }
        return values.get(0).getValue(targetType);
    }

    @Nonnull
    public List<Value> getValues(int targetType) throws RepositoryException {
        if (values.isEmpty()) {
            return Collections.emptyList();
        } else {
            List<Value> vs = Lists.newArrayListWithCapacity(values.size());
            for (TextValue value : values) {
                vs.add(value.getValue(targetType));
            }
            return vs;
        }
    }

    public PropertyState asPropertyState(@Nonnull PropertyDefinition propertyDefinition) throws RepositoryException {
        List<Value> vs = getValues(getTargetType(propertyDefinition));
        PropertyState propertyState;
        if (vs.size() == 1 && !propertyDefinition.isMultiple()) {
            propertyState = PropertyStates.createProperty(name, vs.get(0));
        } else {
            propertyState = PropertyStates.createProperty(name, vs);
        }
        return propertyState;
    }
}
