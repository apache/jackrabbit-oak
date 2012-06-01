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

import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;

class NodeDefinitionTemplateImpl extends ItemDefinitionTemplateImpl
        implements NodeDefinitionTemplate {

    private String defaultPrimaryTypeName = null;

    private String[] requiredPrimaryTypeNames = null;

    private boolean allowSameNameSiblings = false;

    @Override
    public String getDefaultPrimaryTypeName() {
        return defaultPrimaryTypeName;
    }

    @Override
    public void setDefaultPrimaryTypeName(String name) {
        this.defaultPrimaryTypeName  = name;
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        return null;
    }

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return requiredPrimaryTypeNames;
    }

    @Override
    public void setRequiredPrimaryTypeNames(String[] names) {
        this.requiredPrimaryTypeNames = names;
    }

    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        return null;
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return allowSameNameSiblings;
    }

    @Override
    public void setSameNameSiblings(boolean allowSameNameSiblings) {
        this.allowSameNameSiblings = allowSameNameSiblings;
    }

}