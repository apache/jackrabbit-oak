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

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.namepath.NamePathMapper;

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
    private final TextValue[] values;

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
     * @param values value(s) of the property being imported
     */
    public PropInfo(String name, int type, TextValue[] values) {
        this.name = name;
        this.type = type;
        this.values = values;
        multipleStatus = (values.length == 1) ? MultipleStatus.UNKNOWN : MultipleStatus.MULTIPLE;
    }

    /**
     * Creates a property information instance.
     *
     * @param name name of the property being imported
     * @param type type of the property being imported
     * @param values value(s) of the property being imported
     * @param multipleStatus Hint indicating whether the property is
     */
    public PropInfo(String name, int type, TextValue[] values,
                    MultipleStatus multipleStatus) {
        this.name = name;
        this.type = type;
        this.values = values;
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
            return type;
        } else if (type != PropertyType.UNDEFINED) {
            return type;
        } else {
            return PropertyType.STRING;
        }
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public TextValue getTextValue() throws RepositoryException {
        if (multipleStatus == MultipleStatus.MULTIPLE) {
            throw new RepositoryException("TODO");
        }
        return values[0];
    }

    public TextValue[] getTextValues() {
        return values;
    }

    public Value getValue(int targetType, NamePathMapper namePathMapper) throws RepositoryException {
        if (multipleStatus == MultipleStatus.MULTIPLE) {
            throw new RepositoryException("TODO");
        }
        return values[0].getValue(targetType, namePathMapper);
    }

    public Value[] getValues(int targetType, NamePathMapper namePathMapper) throws RepositoryException {
        Value[] va = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            va[i] = values[i].getValue(targetType, namePathMapper);
        }
        return va;
    }
}