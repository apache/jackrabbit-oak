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

import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.oak.namepath.NameMapper;

class NodeDefinitionTemplateImpl extends ItemDefinitionTemplateImpl
        implements NodeDefinitionTemplate {

    private boolean allowSameNameSiblings = false;

    private String defaultPrimaryTypeOakName = null;

    private String[] requiredPrimaryTypeOakNames = null;

    public NodeDefinitionTemplateImpl(NameMapper mapper) {
        super(mapper);
    }

    public NodeDefinitionTemplateImpl(
            NameMapper mapper, NodeDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition);
        setSameNameSiblings(definition.allowsSameNameSiblings());
        setDefaultPrimaryTypeName(definition.getDefaultPrimaryTypeName());
        setRequiredPrimaryTypeNames(definition.getRequiredPrimaryTypeNames());
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return allowSameNameSiblings;
    }

    @Override
    public void setSameNameSiblings(boolean allowSameNameSiblings) {
        this.allowSameNameSiblings = allowSameNameSiblings;
    }

    /**
     * Returns {@code null} since an item definition template is not
     * attached to a live, already registered node type.
     *
     * @return {@code null}
     */
    @Override
    public NodeType getDefaultPrimaryType() {
        return null;
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        return getJcrNameAllowNull(defaultPrimaryTypeOakName);
    }

    @Override
    public void setDefaultPrimaryTypeName(String jcrName)
            throws ConstraintViolationException {
        this.defaultPrimaryTypeOakName =
                    getOakNameAllowNullOrThrowConstraintViolation(jcrName);
    }

    /**
     * Returns {@code null} since an item definition template is not
     * attached to a live, already registered node type.
     *
     * @return {@code null}
     */
    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        return null;
    }

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return getJcrNamesAllowNull(requiredPrimaryTypeOakNames);
    }

    @Override
    public void setRequiredPrimaryTypeNames(String[] jcrNames)
            throws ConstraintViolationException {
        this.requiredPrimaryTypeOakNames =
                getOakNamesOrThrowConstraintViolation(jcrNames);
    }

}
