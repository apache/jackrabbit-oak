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

public class NodeTypeDelegate {

    private final String name;
    private final String[] declaredSuperTypeNames;
    private final String primaryItemName;
    private final boolean isMixin;
    private final boolean isAbstract;
    private final boolean hasOrderableChildNodes;

    private final List<PropertyDefinitionDelegate> declaredPropertyDefinitionDelegates = new ArrayList<PropertyDefinitionDelegate>();
    private final List<NodeDefinitionDelegate> declaredChildNodeDefinitionDelegates = new ArrayList<NodeDefinitionDelegate>();

    public NodeTypeDelegate(String name, String[] declaredSuperTypeNames, String primaryItemName, boolean isMixin,
            boolean isAbstract, boolean hasOrderableChildNodes) {
        this.name = name;
        this.declaredSuperTypeNames = declaredSuperTypeNames;
        this.primaryItemName = primaryItemName;
        this.isMixin = isMixin;
        this.isAbstract = isAbstract;
        this.hasOrderableChildNodes = hasOrderableChildNodes;
    }

    public void addPropertyDefinitionDelegate(PropertyDefinitionDelegate declaredPropertyDefinitionDelegate) {
        this.declaredPropertyDefinitionDelegates.add(declaredPropertyDefinitionDelegate);
    }

    public void addChildNodeDefinitionDelegate(NodeDefinitionDelegate declaredChildNodeDefinitionDelegate) {
        this.declaredChildNodeDefinitionDelegates.add(declaredChildNodeDefinitionDelegate);
    }

    public String[] getDeclaredSuperTypeNames() {
        return declaredSuperTypeNames;
    }

    public String getName() {
        return name;
    }

    public String getPrimaryItemName() {
        return primaryItemName;
    }

    public List<NodeDefinitionDelegate> getChildNodeDefinitionDelegates() {
        return this.declaredChildNodeDefinitionDelegates;
    }

    public List<PropertyDefinitionDelegate> getPropertyDefinitionDelegates() {
        return this.declaredPropertyDefinitionDelegates;
    }

    public boolean hasOrderableChildNodes() {
        return hasOrderableChildNodes;
    }

    public boolean isAbstract() {
        return isAbstract;
    }

    public boolean isMixin() {
        return isMixin;
    }
}
