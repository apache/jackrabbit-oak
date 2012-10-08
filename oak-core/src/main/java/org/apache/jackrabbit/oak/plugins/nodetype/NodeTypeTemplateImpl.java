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
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.value.ValueFactoryImpl;

final class NodeTypeTemplateImpl
    extends AbstractNodeTypeDefinitionBuilder<NodeTypeTemplate>
    implements NodeTypeTemplate {

    private final NodeTypeManager manager;

    private final NameMapper mapper;

    private final ValueFactory factory;

    private String primaryItemName;

    private String[] superTypeNames = new String[0];

    private List<PropertyDefinitionTemplate> propertyDefinitionTemplates;

    private List<NodeDefinitionTemplate> nodeDefinitionTemplates;

    public NodeTypeTemplateImpl(NodeTypeManager manager, NameMapper mapper, ValueFactory factory) {
        this.manager = manager;
        this.mapper = mapper;
        this.factory = factory;
    }

    public NodeTypeTemplateImpl(NameMapper mapper) {
        this(null, mapper, ValueFactoryImpl.getInstance());
    }

    public NodeTypeTemplateImpl(
            NodeTypeManager manager, NameMapper mapper, ValueFactory factory,
            NodeTypeDefinition ntd) throws ConstraintViolationException {
        this(manager, mapper, factory);

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

        getPropertyDefinitionTemplates();  // Make sure propertyDefinitionTemplates is initialised
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

        getNodeDefinitionTemplates();   // Make sure nodeDefinitionTemplates is initialised
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
        return new PropertyDefinitionTemplateImpl(mapper) {
            @Override
            protected Value createValue(String value) {
                return factory.createValue(value);
            }
            @Override
            public void build() {
                getPropertyDefinitionTemplates().add(this);
            }
        };
    }

    @Override
    public NodeDefinitionTemplateImpl newNodeDefinitionBuilder() {
        return new NodeDefinitionTemplateImpl(mapper) {
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
                getNodeDefinitionTemplates().add(this);
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
        this.name = mapper.getJcrName(mapper.getOakName(name));
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
        if (name == null) {
            this.primaryItemName = null;
        }
        else {
            JcrNameParser.checkName(name, false);
            this.primaryItemName = mapper.getJcrName(mapper.getOakName(name));
        }
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        return superTypeNames;
    }

    @Override
    public void setDeclaredSuperTypeNames(String[] names) throws ConstraintViolationException {
        if (names == null) {
            throw new ConstraintViolationException("null is not a valid array of JCR names");
        }
        int k = 0;
        String[] n = new String[names.length];
        for (String name : names) {
            JcrNameParser.checkName(name, false);
            n[k++] = mapper.getJcrName(mapper.getOakName(name));
        }
        this.superTypeNames = n;
    }

    @Override
    public void addSupertype(String name) throws RepositoryException {
        JcrNameParser.checkName(name, false);
        String[] names = new String[superTypeNames.length + 1];
        System.arraycopy(superTypeNames, 0, names, 0, superTypeNames.length);
        names[superTypeNames.length] = mapper.getJcrName(mapper.getOakName(name));
        superTypeNames = names;
    }

    @Override
    public List<PropertyDefinitionTemplate> getPropertyDefinitionTemplates() {
        if (propertyDefinitionTemplates == null) {
            propertyDefinitionTemplates = new ArrayList<PropertyDefinitionTemplate>();
        }
        return propertyDefinitionTemplates;
    }

    @Override
    public List<NodeDefinitionTemplate> getNodeDefinitionTemplates() {
        if (nodeDefinitionTemplates == null) {
            nodeDefinitionTemplates = new ArrayList<NodeDefinitionTemplate>();
        }
        return nodeDefinitionTemplates;
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        return propertyDefinitionTemplates == null
            ? null
            : propertyDefinitionTemplates.toArray(
                new PropertyDefinition[propertyDefinitionTemplates.size()]);
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        return nodeDefinitionTemplates == null
            ? null
            : nodeDefinitionTemplates.toArray(
                new NodeDefinition[nodeDefinitionTemplates.size()]);
    }

}