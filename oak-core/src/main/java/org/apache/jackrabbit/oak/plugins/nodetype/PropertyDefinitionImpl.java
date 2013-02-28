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

import java.util.List;

import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.query.qom.QueryObjectModelConstants;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;

import static javax.jcr.PropertyType.BINARY;
import static javax.jcr.PropertyType.BOOLEAN;
import static javax.jcr.PropertyType.DATE;
import static javax.jcr.PropertyType.DECIMAL;
import static javax.jcr.PropertyType.DOUBLE;
import static javax.jcr.PropertyType.LONG;
import static javax.jcr.PropertyType.NAME;
import static javax.jcr.PropertyType.PATH;
import static javax.jcr.PropertyType.REFERENCE;
import static javax.jcr.PropertyType.STRING;
import static javax.jcr.PropertyType.UNDEFINED;
import static javax.jcr.PropertyType.URI;
import static javax.jcr.PropertyType.WEAKREFERENCE;

/**
 * <pre>
 * [nt:propertyDefinition]
 *   ...
 * - jcr:requiredType (STRING) protected mandatory
 *   < 'STRING', 'URI', 'BINARY', 'LONG', 'DOUBLE',
 *     'DECIMAL', 'BOOLEAN', 'DATE', 'NAME', 'PATH',
 *     'REFERENCE', 'WEAKREFERENCE', 'UNDEFINED'
 * - jcr:valueConstraints (STRING) protected multiple
 * - jcr:defaultValues (UNDEFINED) protected multiple
 * - jcr:multiple (BOOLEAN) protected mandatory
 * - jcr:availableQueryOperators (NAME) protected mandatory multiple
 * - jcr:isFullTextSearchable (BOOLEAN) protected mandatory
 * - jcr:isQueryOrderable (BOOLEAN) protected mandatory
 * </pre>
 */
class PropertyDefinitionImpl extends ItemDefinitionImpl implements PropertyDefinition {

    private static final Value[] NO_VALUES = new Value[0];

    public PropertyDefinitionImpl(
            Tree definition, ValueFactory factory, NamePathMapper mapper) {
        super(definition, factory, mapper);
    }

    /**
     * Returns the numeric constant value of the type with the specified name.
     *
     * In contrast to {@link javax.jcr.PropertyType#valueFromName(String)} this method
     * requires all type names to be all upper case.
     * See also: OAK-294 and http://java.net/jira/browse/JSR_283-811
     *
     * @param name the name of the property type.
     * @return the numeric constant value.
     * @throws IllegalStateException if {@code name} is not a valid property type name.
     */
    public static int valueFromName(String name) {
        if ("STRING".equals(name)) {
            return STRING;
        } else if ("BINARY".equals(name)) {
            return BINARY;
        } else if ("BOOLEAN".equals(name)) {
            return BOOLEAN;
        } else if ("LONG".equals(name)) {
            return LONG;
        } else if ("DOUBLE".equals(name)) {
            return DOUBLE;
        } else if ("DECIMAL".equals(name)) {
            return DECIMAL;
        } else if ("DATE".equals(name)) {
            return DATE;
        } else if ("NAME".equals(name)) {
            return NAME;
        } else if ("PATH".equals(name)) {
            return PATH;
        } else if ("REFERENCE".equals(name)) {
            return REFERENCE;
        } else if ("WEAKREFERENCE".equals(name)) {
            return WEAKREFERENCE;
        } else if ("URI".equals(name)) {
            return URI;
        } else if ("UNDEFINED".equals(name)) {
            return UNDEFINED;
        } else {
            throw new IllegalStateException("unknown property type: " + name);
        }
    }

    //-------------------------------------------------< PropertyDefinition >---

    @Override
    public int getRequiredType() {
        String string = getString(JcrConstants.JCR_REQUIREDTYPE);
        if (string != null) {
            return valueFromName(string);
        } else {
            return UNDEFINED;
        }
    }

    @Override
    public String[] getValueConstraints() {
        // TODO: namespace mapping?
        return getStrings(JcrConstants.JCR_VALUECONSTRAINTS);
    }

    @Override
    public Value[] getDefaultValues() {
        PropertyState property =
                definition.getProperty(JcrConstants.JCR_DEFAULTVALUES);
        if (property == null) {
            return null;
        } else if (property.isArray()) {
            List<Value> values = ValueFactoryImpl.createValues(property, mapper);
            return values.toArray(NO_VALUES);
        } else {
            Value value = ValueFactoryImpl.createValue(property, mapper);
            return new Value[] { value };
        }
    }

    @Override
    public boolean isMultiple() {
        return getBoolean(JcrConstants.JCR_MULTIPLE);
    }

    @Override
    public String[] getAvailableQueryOperators() {
        String[] operators =
                getStrings(NodeTypeConstants.JCR_AVAILABLE_QUERY_OPERATORS);
        if (operators == null) {
            operators = new String[] {
                    QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_NOT_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN,
                    QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN,
                    QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_LIKE };
        }
        return operators;
    }

    @Override
    public boolean isFullTextSearchable() {
        return getBoolean(NodeTypeConstants.JCR_IS_FULLTEXT_SEARCHABLE);
    }

    @Override
    public boolean isQueryOrderable() {
        return getBoolean(NodeTypeConstants.JCR_IS_QUERY_ORDERABLE);
    }

}
