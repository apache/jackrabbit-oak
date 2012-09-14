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

import java.util.ArrayList;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory.AbstractNodeTypeDefinitionBuilder;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.value.ValueFactoryImpl;

final class NodeTypeTemplateImpl
    extends AbstractNodeTypeDefinitionBuilder<NodeTypeTemplate>
    implements NodeTypeTemplate {

    private final NodeTypeManager manager;

    private final ValueFactory factory;

    private String primaryItemName;

    private String[] superTypeNames = new String[0];

    private final List<PropertyDefinitionTemplate> propertyDefinitionTemplates =
            new ArrayList<PropertyDefinitionTemplate>();

    private final List<NodeDefinitionTemplate> nodeDefinitionTemplates =
            new ArrayList<NodeDefinitionTemplate>();

    public NodeTypeTemplateImpl(NodeTypeManager manager, ValueFactory factory) {
        this.manager = manager;
        this.factory = factory;
    }

    public NodeTypeTemplateImpl() {
        this(null, ValueFactoryImpl.getInstance());
    }

    public NodeTypeTemplateImpl(
            NodeTypeManager manager, ValueFactory factory,
            NodeTypeDefinition ntd) throws ConstraintViolationException {
        this(manager, factory);

        setName(ntd.getName());
        setAbstract(ntd.isAbstract());
        setMixin(ntd.isMixin());
        setOrderableChildNodes(ntd.hasOrderableChildNodes());
        setQueryable(ntd.isQueryable());
        String name = ntd.getPrimaryItemName();
        if (name != null) {
            setPrimaryItemName(name);
        }
        setDeclaredSuperTypeNames(ntd.getDeclaredSupertypeNames());

        for (PropertyDefinition pd : ntd.getDeclaredPropertyDefinitions()) {
            PropertyDefinitionTemplateImpl pdt = newPropertyDefinitionBuilder();
            pdt.setDeclaringNodeType(pd.getDeclaringNodeType().getName());
            pdt.setName(pd.getName());
            pdt.setProtected(pd.isProtected());
            pdt.setMandatory(pd.isMandatory());
            pdt.setAutoCreated(pd.isAutoCreated());
            pdt.setOnParentVersion(pd.getOnParentVersion());
            pdt.setMultiple(pd.isMultiple());
            pdt.setRequiredType(pd.getRequiredType());
            pdt.setDefaultValues(pd.getDefaultValues());
            pdt.setValueConstraints(pd.getValueConstraints());
            pdt.setFullTextSearchable(pd.isFullTextSearchable());
            pdt.setAvailableQueryOperators(pd.getAvailableQueryOperators());
            pdt.setQueryOrderable(pd.isQueryOrderable());
            pdt.build();
        }

        for (NodeDefinition nd : ntd.getDeclaredChildNodeDefinitions()) {
            NodeDefinitionTemplateImpl ndt = newNodeDefinitionBuilder();
            ndt.setDeclaringNodeType(nd.getDeclaringNodeType().getName());
            ndt.setName(nd.getName());
            ndt.setProtected(nd.isProtected());
            ndt.setMandatory(nd.isMandatory());
            ndt.setAutoCreated(nd.isAutoCreated());
            ndt.setOnParentVersion(nd.getOnParentVersion());
            ndt.setSameNameSiblings(nd.allowsSameNameSiblings());
            ndt.setDefaultPrimaryTypeName(nd.getDefaultPrimaryTypeName());
            ndt.setRequiredPrimaryTypeNames(nd.getRequiredPrimaryTypeNames());
            ndt.build();
        }
    }

    @Override
    public NodeTypeTemplate build() {
        return this;
    }

    @Override
    public PropertyDefinitionTemplateImpl newPropertyDefinitionBuilder() {
        return new PropertyDefinitionTemplateImpl() {
            @Override
            protected Value createValue(String value) {
                return factory.createValue(value);
            }
            @Override
            public void build() {
                propertyDefinitionTemplates.add(this);
            }
        };
    }

    @Override
    public NodeDefinitionTemplateImpl newNodeDefinitionBuilder() {
        return new NodeDefinitionTemplateImpl() {
            @Override
            protected NodeType getNodeType(String name)
                    throws RepositoryException  {
                if (manager != null) {
                    return manager.getNodeType(name);
                } else {
                    return super.getNodeType(name);
                }
            }
            @Override
            public void build() {
                nodeDefinitionTemplates.add(this);
            }
        };
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) throws ConstraintViolationException {
        JcrNameParser.checkName(name, false);
        this.name = name;
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
    public boolean isQueryable() {
        return queryable;
    }

    @Override
    public void setQueryable(boolean queryable) {
        this.queryable = queryable;
    }

    @Override
    public String getPrimaryItemName() {
        return primaryItemName ;
    }

    @Override
    public void setPrimaryItemName(String name) throws ConstraintViolationException {
        JcrNameParser.checkName(name, false);
        this.primaryItemName = name;
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        return superTypeNames;
    }

    @Override
    public void setDeclaredSuperTypeNames(String[] names) throws ConstraintViolationException {
        for (String name : names) {
            JcrNameParser.checkName(name, false);
        }
        this.superTypeNames = names;
    }

    @Override
    public void addSupertype(String name) throws RepositoryException {
        JcrNameParser.checkName(name, false);
        String[] names = new String[superTypeNames.length + 1];
        System.arraycopy(superTypeNames, 0, names, 0, superTypeNames.length);
        names[superTypeNames.length] = name;
        superTypeNames = names;
    }

    @Override
    public List<PropertyDefinitionTemplate> getPropertyDefinitionTemplates() {
        return propertyDefinitionTemplates;
    }

    @Override
    public List<NodeDefinitionTemplate> getNodeDefinitionTemplates() {
        return nodeDefinitionTemplates;
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        return propertyDefinitionTemplates.toArray(
                new PropertyDefinition[propertyDefinitionTemplates.size()]);
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        return nodeDefinitionTemplates.toArray(
                new NodeDefinition[nodeDefinitionTemplates.size()]);
    }

}