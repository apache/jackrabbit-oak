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

import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_ABORT;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_COMPUTE;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_COPY;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_IGNORE;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_INITIALIZE;
import static javax.jcr.version.OnParentVersionAction.ACTIONNAME_VERSION;

import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.OnParentVersionAction;

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

    private final NodeType type;

    protected final NodeUtil node;

    protected ItemDefinitionImpl(NodeType type, NodeUtil node) {
        this.type = type;
        this.node = node;
    }

    @Override
    public NodeType getDeclaringNodeType() {
        return type;
    }

    @Override
    public String getName() {
        return node.getName("jcr:name", "*");
    }

    @Override
    public boolean isAutoCreated() {
        return node.getBoolean("jcr:autoCreated");
    }

    @Override
    public boolean isMandatory() {
        return node.getBoolean("jcr:mandatory");
    }

    @Override
    public int getOnParentVersion() {
        String opv = node.getString("jcr:onParentVersion", ACTIONNAME_COPY);
        if (ACTIONNAME_ABORT.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.ABORT;
        } else if (ACTIONNAME_COMPUTE.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.COMPUTE;
        } else if (ACTIONNAME_IGNORE.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.IGNORE;
        } else if (ACTIONNAME_INITIALIZE.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.INITIALIZE;
        } else if (ACTIONNAME_VERSION.equalsIgnoreCase(opv)) {
            return OnParentVersionAction.VERSION;
        } else {
            return OnParentVersionAction.COPY;
        }
    }

    @Override
    public boolean isProtected() {
        return node.getBoolean("jcr:protected");
    }

}
