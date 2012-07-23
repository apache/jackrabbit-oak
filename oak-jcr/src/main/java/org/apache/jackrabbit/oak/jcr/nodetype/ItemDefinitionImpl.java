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

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

/**
 * Adapter class for turning an in-content property or node definition
 * node ("nt:propertyDefinition" or "nt:childNodeDefinition") to an
 * {@link ItemDefinition} instance.
 */
class ItemDefinitionImpl extends TypeNode implements ItemDefinition {

    private final NodeType type;

    protected ItemDefinitionImpl(NodeType type, Node node) {
        super(node);
        this.type = type;
    }

    protected void appendItemCND(
            StringBuilder sb, String requiredType, String defaultValue) {
        sb.append(getName());
        if (requiredType != null) {
            sb.append(" (").append(requiredType).append(")");
        }
        if (defaultValue != null) {
            sb.append(" = ").append(defaultValue);
        }
        if (isAutoCreated()) {
            sb.append(" autocreated");
        }
        if (isMandatory()) {
            sb.append(" mandatory");
        }
        if (isProtected()) {
            sb.append(" protected");
        }
        int opv = getOnParentVersion();
        if (opv != OnParentVersionAction.COPY) {
            sb.append(" ").append(OnParentVersionAction.nameFromValue(opv));
        }
    }

    //----------------------------------------------------< ItemDefinition >--

    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    /** CND: <pre>- jcr:name (NAME) protected</pre> */
    @Override
    public String getName() {
        return getString(Property.JCR_NAME, "*");
    }

    /** CND: <pre>- jcr:autoCreated (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isAutoCreated() {
        return getBoolean(Property.JCR_AUTOCREATED);
    }

    /** CND: <pre>- jcr:mandatory (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isMandatory() {
        return getBoolean(Property.JCR_MANDATORY);
    }

    /**
     * CND:
     * <pre>
     * - jcr:onParentVersion (STRING) protected mandatory
     *   &lt; 'COPY', 'VERSION', 'INITIALIZE', 'COMPUTE', 'IGNORE', 'ABORT'
     * </pre>
     */
    @Override
    public int getOnParentVersion() {
        try {
            return OnParentVersionAction.valueFromName(
                    getString(Property.JCR_ON_PARENT_VERSION));
        } catch (IllegalArgumentException e) {
            throw illegalState(e);
        }
    }

    /** CND: <pre>- jcr:protected (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isProtected() {
        return getBoolean(Property.JCR_PROTECTED);
    }

}