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
package org.apache.jackrabbit.oak.plugins.type;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractPropertyDefinitionBuilder;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;

class PropertyDefinitionTemplateImpl
        extends AbstractPropertyDefinitionBuilder<NodeTypeTemplate>
        implements PropertyDefinitionTemplate {

    private String[] valueConstraints;

    private Value[] defaultValues;

    public PropertyDefinitionTemplateImpl() {
        onParent = OnParentVersionAction.COPY;
        requiredType = PropertyType.STRING;
    }

    protected Value createValue(String value) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public void build() {
        // do nothing by default
    }

    @Override
    public NodeType getDeclaringNodeType() {
        return null;
    }

    @Override
    public void setDeclaringNodeType(String name) {
        // ignore
    }

    @Override
    public void setName(String name) throws ConstraintViolationException {
        JcrNameParser.checkName(name, true);
        this.name = name;
    }

    @Override
    public boolean isAutoCreated() {
        return autocreate;
    }

    @Override
    public void setAutoCreated(boolean autocreate) {
        this.autocreate = autocreate;
    }

    @Override
    public boolean isProtected() {
        return isProtected;
    }

    @Override
    public void setProtected(boolean isProtected) {
        this.isProtected = isProtected;
    }

    @Override
    public boolean isMandatory() {
        return isMandatory;
    }

    @Override
    public void setMandatory(boolean isMandatory) {
        this.isMandatory = isMandatory;
    }

    @Override
    public int getOnParentVersion() {
        return onParent;
    }

    @Override
    public void setOnParentVersion(int onParent) {
        this.onParent = onParent;
    }

    @Override
    public void setRequiredType(int requiredType) {
        this.requiredType = requiredType;
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
    public boolean isQueryOrderable() {
        return queryOrderable;
    }

    @Override
    public void setQueryOrderable(boolean queryOrderable) {
        this.queryOrderable = queryOrderable;
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
    public String[] getAvailableQueryOperators() {
        return queryOperators;
    }

    @Override
    public void setAvailableQueryOperators(String[] queryOperators) {
        this.queryOperators = queryOperators;
    }

    @Override
    public Value[] getDefaultValues() {
        return defaultValues;
    }

    @Override
    public void setDefaultValues(Value[] defaultValues) {
        this.defaultValues = defaultValues;
    }

    @Override
    public void addDefaultValues(String value) throws RepositoryException {
        if (defaultValues == null) {
            defaultValues = new Value[] { createValue(value) };
        } else {
            Value[] values = new Value[defaultValues.length + 1];
            System.arraycopy(defaultValues, 0, values, 0, defaultValues.length);
            values[defaultValues.length] = createValue(value);
            defaultValues = values;
        }
    }

    @Override
    public String[] getValueConstraints() {
        return valueConstraints;
    }

    @Override
    public void setValueConstraints(String[] constraints) {
        this.valueConstraints = constraints;
    }

    @Override
    public void addValueConstraint(String constraint) {
        if (valueConstraints == null) {
            valueConstraints = new String[] { constraint };
        } else {
            String[] constraints = new String[valueConstraints.length + 1];
            System.arraycopy(valueConstraints, 0, constraints, 0, valueConstraints.length);
            constraints[valueConstraints.length] = constraint;
            valueConstraints = constraints;
        }
    }

}