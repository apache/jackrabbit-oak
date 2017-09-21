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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

import static com.google.common.base.Preconditions.checkNotNull;

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
class ItemDefinitionImpl extends AbstractTypeDefinition
        implements ItemDefinition {

    private final NodeType type;

    protected ItemDefinitionImpl(
            Tree definition, NodeType type, NamePathMapper mapper) {
        super(definition, mapper);
        this.type = checkNotNull(type);
    }

    //-----------------------------------------------------< ItemDefinition >---

    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    @Override
    public String getName() {
        String oakName = getName(JcrConstants.JCR_NAME);
        if (oakName != null) {
            return mapper.getJcrName(oakName);
        } else {
            return NodeTypeConstants.RESIDUAL_NAME;
        }
    }

    @Override
    public boolean isAutoCreated() {
        return getBoolean(JcrConstants.JCR_AUTOCREATED);
    }

    @Override
    public boolean isMandatory() {
        return getBoolean(JcrConstants.JCR_MANDATORY);
    }

    @Override
    public int getOnParentVersion() {
        String action = getString(JcrConstants.JCR_ONPARENTVERSION);
        if (action != null) {
            return OnParentVersionAction.valueFromName(action);
        } else {
            return OnParentVersionAction.COPY;
        }
    }

    @Override
    public boolean isProtected() {
        return getBoolean(JcrConstants.JCR_PROTECTED);
    }

    @Override
    public String toString() {
        return getName();
    }

}
