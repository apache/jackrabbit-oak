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

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.query.qom.QueryObjectModelConstants;

import org.apache.jackrabbit.oak.namepath.NameMapper;

class PropertyDefinitionTemplateImpl extends ItemDefinitionTemplateImpl
        implements PropertyDefinitionTemplate {

    private static final String[] ALL_OPERATORS = new String[]{
        QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN,
        QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN,
        QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_LIKE,
        QueryObjectModelConstants.JCR_OPERATOR_NOT_EQUAL_TO
    };

    private int requiredType = PropertyType.STRING;

    private boolean isMultiple = false;

    private boolean fullTextSearchable = true;

    private boolean queryOrderable = true;

    private String[] queryOperators = ALL_OPERATORS;

    private String[] valueConstraints = null;

    private Value[] defaultValues = null;

    PropertyDefinitionTemplateImpl(NameMapper mapper) {
        super(mapper);
    }

    public PropertyDefinitionTemplateImpl(
            NameMapper mapper, PropertyDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition);
        setRequiredType(definition.getRequiredType());
        setMultiple(definition.isMultiple());
        setFullTextSearchable(definition.isFullTextSearchable());
        setQueryOrderable(definition.isQueryOrderable());
        setAvailableQueryOperators(definition.getAvailableQueryOperators());
        setValueConstraints(definition.getValueConstraints());
        setDefaultValues(definition.getDefaultValues());
    }

    @Override
    public int getRequiredType() {
        return requiredType;
    }

    @Override
    public void setRequiredType(int type) {
        PropertyType.nameFromValue(type); // validation
        this.requiredType = type;
    }

    @Override
    public boolean isMultiple() {
        return isMultiple;
    }

    @Override
    public void setMultiple(boolean isMultiple) {
        this.isMultiple = isMultiple;
    }

    @Override
    public boolean isFullTextSearchable() {
        return fullTextSearchable;
    }

    @Override
    public void setFullTextSearchable(boolean fullTextSearchable) {
        this.fullTextSearchable = fullTextSearchable;
    }

    @Override
    public boolean isQueryOrderable() {
        return queryOrderable;
    }

    @Override
    public void setQueryOrderable(boolean queryOrderable) {
        this.queryOrderable = queryOrderable;
    }

    @Override
    public String[] getAvailableQueryOperators() {
        return queryOperators;
    }

    @Override
    public void setAvailableQueryOperators(String[] operators) {
        checkNotNull(operators);
        this.queryOperators = new String[operators.length];
        System.arraycopy(operators, 0, this.queryOperators, 0, operators.length);
    }

    @Override
    public String[] getValueConstraints() {
        return valueConstraints; // no problem if modified by client
    }

    @Override
    public void setValueConstraints(String[] constraints) {
        if (constraints == null) {
            this.valueConstraints = null;
        } else {
            this.valueConstraints = new String[constraints.length];
            System.arraycopy(
                    constraints, 0, valueConstraints, 0, constraints.length);
        }
    }

    @Override
    public Value[] getDefaultValues() {
        return defaultValues; // no problem if modified by client
    }

    @Override
    public void setDefaultValues(Value[] values) {
        if (values == null) {
            this.defaultValues = null;
        } else {
            this.defaultValues = new Value[values.length];
            System.arraycopy(values, 0, defaultValues, 0, values.length);
        }
    }

}
