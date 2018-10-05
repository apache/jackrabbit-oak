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

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.OnParentVersionAction;

/**
 * Node definition for the root node.
 */
final class RootNodeDefinition implements NodeDefinition {

    private static final String REP_ROOT = "rep:root";

    private final NodeTypeManager ntManager;

    RootNodeDefinition(NodeTypeManager ntManager) {
        this.ntManager = ntManager;
    }

    //-----------------------------------------------------< NodeDefinition >---
    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        try {
            return new NodeType[] {ntManager.getNodeType(REP_ROOT)};
        } catch (RepositoryException e) {
            return new NodeType[0];
        }
    }

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return new String[] {REP_ROOT};
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        try {
            return ntManager.getNodeType(REP_ROOT);
        } catch (RepositoryException e) {
            return null;
        }
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        return REP_ROOT;
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return false;
    }

    @Override
    public NodeType getDeclaringNodeType() {
        try {
            return ntManager.getNodeType(REP_ROOT);
        } catch (RepositoryException e) {
            return null;
        }
    }

    @Override
    public String getName() {
        return REP_ROOT;
    }

    @Override
    public boolean isAutoCreated() {
        return true;
    }

    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    public int getOnParentVersion() {
        return OnParentVersionAction.VERSION;
    }

    @Override
    public boolean isProtected() {
        return false;
    }
}