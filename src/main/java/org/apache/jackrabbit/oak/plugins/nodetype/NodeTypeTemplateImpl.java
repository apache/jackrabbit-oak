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

import javax.annotation.Nonnull;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import org.apache.jackrabbit.oak.namepath.NameMapper;

import com.google.common.collect.Lists;

final class NodeTypeTemplateImpl extends AbstractNamedTemplate
        implements NodeTypeTemplate {

    private static final PropertyDefinition[] EMPTY_PROPERTY_DEFINITION_ARRAY =
            new PropertyDefinition[0];

    private static final NodeDefinition[] EMPTY_NODE_DEFINITION_ARRAY =
            new NodeDefinition[0];

    protected boolean isMixin;

    protected boolean isOrderable;

    protected boolean isAbstract;

    protected boolean queryable;

    private String primaryItemOakName = null; // not defined by default

    @Nonnull
    private String[] superTypeOakNames = new String[0];

    private List<PropertyDefinitionTemplate> propertyDefinitionTemplates = null;

    private List<NodeDefinitionTemplate> nodeDefinitionTemplates = null;

    public NodeTypeTemplateImpl(NameMapper mapper) {
        super(mapper);
    }

    NodeTypeTemplateImpl(NameMapper mapper, NodeTypeDefinition definition)
            throws ConstraintViolationException {
        super(mapper, definition.getName());

        setMixin(definition.isMixin());
        setOrderableChildNodes(definition.hasOrderableChildNodes());
        setAbstract(definition.isAbstract());
        setQueryable(definition.isQueryable());
        String primaryItemName = definition.getPrimaryItemName();
        if (primaryItemName != null) {
            setPrimaryItemName(primaryItemName);
        }
        setDeclaredSuperTypeNames(definition.getDeclaredSupertypeNames());

        PropertyDefinition[] pds = definition.getDeclaredPropertyDefinitions();
        if (pds != null) {
            propertyDefinitionTemplates =
                    Lists.newArrayListWithCapacity(pds.length);
            for (int i = 0; pds != null && i < pds.length; i++) {
                propertyDefinitionTemplates.add(
                        new PropertyDefinitionTemplateImpl(mapper, pds[i]));
            }
        }

        NodeDefinition[] nds = definition.getDeclaredChildNodeDefinitions();
        if (nds != null) {
            nodeDefinitionTemplates =
                    Lists.newArrayListWithCapacity(nds.length);
            for (int i = 0; i < nds.length; i++) {
                nodeDefinitionTemplates.add(
                        new NodeDefinitionTemplateImpl(mapper, nds[i]));
            }
        }
    }

    @Override
    public boolean isMixin() {
        return isMixin;
    }

    @Override
    public void setMixin(boolean mixin) {
        this.isMixin = mixin;
    }

    @Override
    public boolean hasOrderableChildNodes() {
        return isOrderable ;
    }

    @Override
    public void setOrderableChildNodes(boolean orderable) {
        this.isOrderable = orderable;
    }

    @Override
    public boolean isAbstract() {
        return isAbstract;
    }

    @Override
    public void setAbstract(boolean abstractStatus) {
        this.isAbstract = abstractStatus;
    }

    @Override
    public boolean isQueryable() {
        return queryable;
    }

    @Override
    public void setQueryable(boolean queryable) {
        this.queryable = queryable;
    }

    @Override
    public String getPrimaryItemName() {
        return getJcrNameAllowNull(primaryItemOakName);
    }

    @Override
    public void setPrimaryItemName(String jcrName)
            throws ConstraintViolationException {
        this.primaryItemOakName =
                getOakNameAllowNullOrThrowConstraintViolation(jcrName);
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        return getJcrNamesAllowNull(superTypeOakNames);
    }

    @Override
    public void setDeclaredSuperTypeNames(String[] jcrNames)
            throws ConstraintViolationException {
        this.superTypeOakNames =
                getOakNamesOrThrowConstraintViolation(jcrNames);
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        if (propertyDefinitionTemplates != null) {
            return propertyDefinitionTemplates.toArray(
                    EMPTY_PROPERTY_DEFINITION_ARRAY);
        } else {
            return null;
        }
    }

    @Override
    public List<PropertyDefinitionTemplate> getPropertyDefinitionTemplates() {
        if (propertyDefinitionTemplates == null) {
            propertyDefinitionTemplates = Lists.newArrayList();
        }
        return propertyDefinitionTemplates;
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        if (nodeDefinitionTemplates != null) {
            return nodeDefinitionTemplates.toArray(
                    EMPTY_NODE_DEFINITION_ARRAY);
        } else {
            return null;
        }
    }

    @Override
    public List<NodeDefinitionTemplate> getNodeDefinitionTemplates() {
        if (nodeDefinitionTemplates == null) {
            nodeDefinitionTemplates = Lists.newArrayList();
        }
        return nodeDefinitionTemplates;
    }

}