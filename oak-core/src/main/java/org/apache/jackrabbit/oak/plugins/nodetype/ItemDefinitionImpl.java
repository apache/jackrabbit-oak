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

import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * [nt:{propertyDefinition,childNodeDefinition}]
 * - jcr:name (NAME) protected 
 * - jcr:autoCreated (BOOLEAN) protected mandatory
 * - jcr:mandatory (BOOLEAN) protected mandatory
 * - jcr:onParentVersion (STRING) protected mandatory
 *     < 'COPY', 'VERSION', 'INITIALIZE', 'COMPUTE', 'IGNORE', 'ABORT'
 * - jcr:protected (BOOLEAN) protected mandatory
 *   ...
 * </pre>
 */
class ItemDefinitionImpl implements ItemDefinition {

    private static final Logger log = LoggerFactory.getLogger(ItemDefinitionImpl.class);

    private final NodeType type;

    protected final NodeUtil node;

    protected ItemDefinitionImpl(NodeType type, NodeUtil node) {
        this.type = type;
        this.node = node;
    }

    //-----------------------------------------------------< ItemDefinition >---
    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    @Override
    public String getName() {
        return node.getName(JcrConstants.JCR_NAME, NodeTypeConstants.RESIDUAL_NAME);
    }

    @Override
    public boolean isAutoCreated() {
        return node.getBoolean(JcrConstants.JCR_AUTOCREATED);
    }

    @Override
    public boolean isMandatory() {
        return node.getBoolean(JcrConstants.JCR_MANDATORY);
    }

    @Override
    public int getOnParentVersion() {
        try {
            return OnParentVersionAction.valueFromName(node.getString(
                    "jcr:onParentVersion",
                    OnParentVersionAction.ACTIONNAME_COPY));
        } catch (IllegalArgumentException e) {
            log.warn("Unexpected jcr:onParentVersion value", e);
            return OnParentVersionAction.COPY;
        }
    }

    @Override
    public boolean isProtected() {
        return node.getBoolean(JcrConstants.JCR_PROTECTED);
    }

    @Override
    public String toString() {
        return getName();
    }

    //-----------------------------------------------------------< internal >---
    String getOakName() {
        return node.getString(JcrConstants.JCR_NAME, NodeTypeConstants.RESIDUAL_NAME);
    }
}
