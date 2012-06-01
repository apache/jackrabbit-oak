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

import java.util.ArrayList;
import java.util.List;

import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

class NodeTypeTemplateImpl implements NodeTypeTemplate {

    private String name = null;

    private boolean isAbstract = false;

    private boolean isMixin = false;

    private boolean isOrderable = false;

    private boolean isQueryable = true;

    private String primaryItemName = null;

    private String[] superTypeNames = new String[0];

    private List<PropertyDefinitionTemplate> propertyDefinitionTemplates =
            new ArrayList<PropertyDefinitionTemplate>();

    private List<NodeDefinitionTemplate> nodeDefinitionTemplates =
            new ArrayList<NodeDefinitionTemplate>();

    public NodeTypeTemplateImpl() {
    }

    public NodeTypeTemplateImpl(NodeTypeDefinition ntd) {
        this.name = ntd.getName();
        this.isAbstract = ntd.isAbstract();
        this.isMixin = ntd.isMixin();
        this.isOrderable = ntd.hasOrderableChildNodes();
        this.isQueryable = ntd.isQueryable();
        this.primaryItemName = ntd.getPrimaryItemName();
        this.superTypeNames = ntd.getDeclaredSupertypeNames();
        // TODO: child item templates?
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
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
        return isQueryable;
    }

    @Override
    public void setQueryable(boolean queryable) {
        this.isQueryable = queryable;
    }

    @Override
    public String getPrimaryItemName() {
        return primaryItemName ;
    }

    @Override
    public void setPrimaryItemName(String name) {
        this.primaryItemName = name;
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        return superTypeNames;
    }

    @Override
    public void setDeclaredSuperTypeNames(String[] names) {
        this.superTypeNames = names;
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
        return null;
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        return null;
    }

}