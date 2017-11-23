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

import static javax.jcr.PropertyType.UNDEFINED;

import java.util.List;

import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.query.qom.QueryObjectModelConstants;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

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

    PropertyDefinitionImpl(Tree definition, NodeType type, NamePathMapper mapper) {
        super(definition, type, mapper);
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
        Type<?> type = Type.fromString(name);
        if (type.isArray()) {
            throw new IllegalStateException("unknown property type: " + name);
        }
        return type.tag();
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
        String[] valConstraints = getStrings(JcrConstants.JCR_VALUECONSTRAINTS);
        if (valConstraints != null) {
            return valConstraints;
        } else {
            return new String[0];
        }
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
