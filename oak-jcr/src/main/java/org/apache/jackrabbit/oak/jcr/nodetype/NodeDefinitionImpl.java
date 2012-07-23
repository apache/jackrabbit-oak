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

import javax.annotation.CheckForNull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import com.google.common.base.Joiner;

/**
 * Adapter class for turning an in-content property definition
 * node ("nt:childNodeDefinition") to a {@link NodeDefinition} instance.
 */
class NodeDefinitionImpl extends ItemDefinitionImpl implements NodeDefinition {

    private final NodeTypeManager manager;

    protected NodeDefinitionImpl(
            NodeTypeManager manager, NodeType type, Node node) {
        super(type, node);
        this.manager = manager;
    }

    /** CND: <pre>- jcr:requiredPrimaryTypes (NAME) = 'nt:base' protected mandatory multiple</pre> */
    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return getStrings(Property.JCR_REQUIRED_PRIMARY_TYPES);
    }

    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        String[] names = getRequiredPrimaryTypeNames();
        List<NodeType> types = new ArrayList<NodeType>(names.length);
        for (int i = 0; i < names.length; i++) {
            types.add(getType(manager, names[i]));
        }
        return types.toArray(new NodeType[types.size()]);
    }

    /** CND: <pre>- jcr:defaultPrimaryType (NAME) protected</pre> */
    @Override @CheckForNull
    public String getDefaultPrimaryTypeName() {
        return getString(Property.JCR_DEFAULT_PRIMARY_TYPE, null);
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        String name = getDefaultPrimaryTypeName();
        if (name != null) {
            return getType(manager, name);
        }
        return null;
    }

    /** CND: <pre>- jcr:sameNameSiblings (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean allowsSameNameSiblings() {
        return getBoolean(Property.JCR_SAME_NAME_SIBLINGS);
    }

    //------------------------------------------------------------< Object >--

    public String toString() {
        String rt = null;
        String[] rts = getRequiredPrimaryTypeNames();
        if (rts != null) {
            rt = Joiner.on(", ").join(rts);
        }

        StringBuilder sb = new StringBuilder("+ ");
        appendItemCND(sb, rt, getDefaultPrimaryTypeName());
        sb.append(" ..."); // TODO: rest of the info
        return sb.toString();
    }

}
