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
package org.apache.jackrabbit.oak.jcr.nodetype;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

class PropertyDefinitionTemplateImpl extends ItemDefinitionTemplateImpl
        implements PropertyDefinitionTemplate {

    private boolean isMultiple = false;

    private Value[] defaultValues = null;

    private String[] availableQueryOperators = new String[0];

    private int requiredType = PropertyType.STRING;

    private boolean fullTextSearchable = true;

    private boolean queryOrderable = true;

    private String[] valueConstraints = null;

    @Override
    public boolean isMultiple() {
        return isMultiple;
    }

    @Override
    public void setMultiple(boolean isMultiple) {
        this.isMultiple = isMultiple;
    }

    @Override
    public String[] getValueConstraints() {
        return valueConstraints ;
    }

    @Override
    public void setValueConstraints(String[] constraints) {
        this.valueConstraints = constraints;
    }

    @Override
    public boolean isQueryOrderable() {
        return queryOrderable ;
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
    public int getRequiredType() {
        return requiredType ;
    }

    @Override
    public void setRequiredType(int type) {
        this.requiredType = type;
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
    public String[] getAvailableQueryOperators() {
        return availableQueryOperators ;
    }

    @Override
    public void setAvailableQueryOperators(String[] operators) {
        this.availableQueryOperators = operators;
    }

}