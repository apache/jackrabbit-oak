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

import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.namepath.NameMapper;

class PropertyDefinitionImpl extends ItemDefinitionImpl implements PropertyDefinition {

    private final PropertyDefinitionDelegate dlg;

    private final ValueFactoryImpl vfac;

    public PropertyDefinitionImpl(NodeType type, NameMapper mapper, ValueFactoryImpl vfac, PropertyDefinitionDelegate delegate) {
        super(type, mapper, delegate);
        this.vfac = vfac;
        this.dlg = delegate;
    }

    @Override
    public int getRequiredType() {
        return dlg.getRequiredType();
    }

    @Override
    public String[] getValueConstraints() {
        return new String[0];
    }

    @Override
    public Value[] getDefaultValues() {
        CoreValue[] defaults = dlg.getDefaultCoreValues();
        Value[] result = new Value[defaults.length];
        for (int i = 0; i < defaults.length; i++) {
            result[i] = vfac.createValue(defaults[i]);
        }
        return result;
    }

    @Override
    public boolean isMultiple() {
        return dlg.isMultiple();
    }

    @Override
    public String[] getAvailableQueryOperators() {
        return new String[0];
    }

    @Override
    public boolean isFullTextSearchable() {
        return false;
    }

    @Override
    public boolean isQueryOrderable() {
        return false;
    }

}
